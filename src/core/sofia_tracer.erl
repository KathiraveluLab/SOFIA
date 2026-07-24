-module(sofia_tracer).
-behaviour(gen_server).

-export([start_link/0, start_span/3, start_span/4, end_span/2, get_trace/1, clear/0, generate_id/0,
         get_sampling_rate/0, get_metrics/0, set_target_rate/1, set_max_memory/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(span, {
    span_id :: binary(),
    trace_id :: binary(),
    parent_span_id :: binary() | undefined,
    name :: atom() | string(),
    node :: node(),
    start_time :: integer(), %% microseconds
    end_time :: integer() | undefined,
    duration :: integer() | undefined %% microseconds
}).

-record(state, {
    r_target = 1000.0 :: float(),       %% Target telemetry ingestion throughput (spans/sec)
    m_max = 100000000.0 :: float(),     %% Maximum Mnesia telemetry memory ceiling (bytes)
    window_start = 0 :: integer(),      %% Microseconds/seconds timestamp of current window
    request_count = 0 :: integer(),     %% Count of span requests in current window
    r_current = 0.0 :: float(),         %% Observed request rate (spans/sec)
    sampled_spans = #{} :: map()        %% Map of SpanId -> boolean() decision
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_span(TraceId, Name, ParentSpanId) ->
    SpanId = generate_id(),
    start_span(TraceId, SpanId, Name, ParentSpanId).

start_span(TraceId, SpanId, Name, ParentSpanId) ->
    StartTime = erlang:system_time(microsecond),
    Span = #span{
        span_id = SpanId,
        trace_id = TraceId,
        parent_span_id = ParentSpanId,
        name = Name,
        node = node(),
        start_time = StartTime
    },
    gen_server:call(?MODULE, {start_span, Span}),
    {ok, SpanId}.

end_span(TraceId, SpanId) ->
    EndTime = erlang:system_time(microsecond),
    gen_server:call(?MODULE, {end_span, TraceId, SpanId, EndTime}).

get_trace(TraceId) ->
    gen_server:call(?MODULE, {get_trace, TraceId}).

clear() ->
    gen_server:call(?MODULE, clear).

get_sampling_rate() ->
    gen_server:call(?MODULE, get_sampling_rate).

get_metrics() ->
    gen_server:call(?MODULE, get_metrics).

set_target_rate(Rate) ->
    gen_server:call(?MODULE, {set_target_rate, Rate}).

set_max_memory(MaxMem) ->
    gen_server:call(?MODULE, {set_max_memory, MaxMem}).

%% Gen_server callbacks
init([]) ->
    RTarget = application:get_env(sofia, trace_target_rate, 1000.0),
    MMax = application:get_env(sofia, trace_max_memory, 100000000.0),
    NowSec = erlang:system_time(second),
    {ok, #state{
        r_target = float(RTarget),
        m_max = float(MMax),
        window_start = NowSec,
        request_count = 0,
        r_current = 0.0,
        sampled_spans = #{}
    }}.

handle_call({start_span, Span}, _From, State) ->
    NowSec = erlang:system_time(second),
    WindowStart = State#state.window_start,
    RequestCount = State#state.request_count + 1,
    {NewWindowStart, NewRequestCount, RCurrent} = if
        NowSec > WindowStart ->
            Elapsed = erlang:max(1, NowSec - WindowStart),
            Rate = RequestCount / float(Elapsed),
            {NowSec, 1, Rate};
        true ->
            Rate = erlang:max(State#state.r_current, RequestCount / 1.0),
            {WindowStart, RequestCount, Rate}
    end,

    MMnesia = get_mnesia_span_memory_bytes(),
    Sigma = calculate_sigma(RCurrent, State#state.r_target, MMnesia, State#state.m_max),
    Rand = rand:uniform(),
    ShouldSample = (Rand =< Sigma),
    SpanId = Span#span.span_id,
    NewSampledMap = maps:put(SpanId, ShouldSample, State#state.sampled_spans),

    Reply = case ShouldSample of
        true ->
            F = fun() -> mnesia:write(span, Span, write) end,
            mnesia:transaction(F),
            ok;
        false ->
            dropped
    end,

    NewState = State#state{
        window_start = NewWindowStart,
        request_count = NewRequestCount,
        r_current = RCurrent,
        sampled_spans = NewSampledMap
    },
    {reply, Reply, NewState};

handle_call({end_span, TraceId, SpanId, EndTime}, _From, State) ->
    ShouldSample = maps:get(SpanId, State#state.sampled_spans, true),
    NewSampledMap = maps:remove(SpanId, State#state.sampled_spans),
    Res = case ShouldSample of
        true ->
            F = fun() ->
                case mnesia:read(span, SpanId, write) of
                    [Span] when Span#span.trace_id =:= TraceId ->
                        Duration = EndTime - Span#span.start_time,
                        UpdatedSpan = Span#span{end_time = EndTime, duration = Duration},
                        mnesia:write(span, UpdatedSpan, write),
                        ok;
                    _ ->
                        {error, not_found}
                end
            end,
            case mnesia:transaction(F) of
                {atomic, Result} -> Result;
                {aborted, _Reason} -> {error, mnesia_error}
            end;
        false ->
            ok
    end,
    {reply, Res, State#state{sampled_spans = NewSampledMap}};

handle_call({get_trace, TraceId}, _From, State) ->
    F = fun() ->
        Pattern = #span{trace_id = TraceId, _ = '_'},
        mnesia:match_object(Pattern)
    end,
    Spans = case mnesia:transaction(F) of
        {atomic, List} -> List;
        _ -> []
    end,
    Sorted = lists:keysort(#span.start_time, Spans),
    MapSpans = [#{
        trace_id => S#span.trace_id,
        span_id => S#span.span_id,
        parent_span_id => S#span.parent_span_id,
        name => S#span.name,
        node => S#span.node,
        start_time => S#span.start_time,
        end_time => S#span.end_time,
        duration => S#span.duration
    } || S <- Sorted],
    {reply, MapSpans, State};

handle_call(clear, _From, State) ->
    F = fun() -> mnesia:clear_table(span) end,
    mnesia:transaction(F),
    {reply, ok, State#state{sampled_spans = #{}}};

handle_call(get_sampling_rate, _From, State) ->
    MMnesia = get_mnesia_span_memory_bytes(),
    Sigma = calculate_sigma(State#state.r_current, State#state.r_target, MMnesia, State#state.m_max),
    {reply, {ok, Sigma}, State};

handle_call(get_metrics, _From, State) ->
    MMnesia = get_mnesia_span_memory_bytes(),
    Sigma = calculate_sigma(State#state.r_current, State#state.r_target, MMnesia, State#state.m_max),
    Metrics = #{
        r_current => State#state.r_current,
        r_target => State#state.r_target,
        m_mnesia => MMnesia,
        m_max => State#state.m_max,
        sigma => Sigma
    },
    {reply, {ok, Metrics}, State};

handle_call({set_target_rate, Rate}, _From, State) ->
    {reply, ok, State#state{r_target = float(Rate)}};

handle_call({set_max_memory, MaxMem}, _From, State) ->
    {reply, ok, State#state{m_max = float(MaxMem)}};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

generate_id() ->
    Bin = crypto:strong_rand_bytes(8),
    list_to_binary([hex_digit(N) || <<N:4>> <= Bin]).

hex_digit(N) when N < 10 -> $0 + N;
hex_digit(N) -> $a + N - 10.

get_mnesia_span_memory_bytes() ->
    try mnesia:table_info(span, memory) of
        Words when is_integer(Words) ->
            float(Words * erlang:system_info(wordsize));
        _ ->
            0.0
    catch
        _:_ -> 0.0
    end.

calculate_sigma(RCurrent, RTarget, MMnesia, MMax) ->
    RateFactor = if
        RCurrent =< 0.0 -> 1.0;
        true -> erlang:min(1.0, RTarget / erlang:max(1.0, RCurrent))
    end,
    MemFactor = if
        MMax > 0.0 andalso MMnesia >= MMax -> 0.0;
        MMax > 0.0 -> 1.0 - (MMnesia / MMax);
        true -> 1.0
    end,
    erlang:max(0.0, erlang:min(1.0, RateFactor * MemFactor)).


