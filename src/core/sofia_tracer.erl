-module(sofia_tracer).
-behaviour(gen_server).

-export([start_link/0, start_span/3, start_span/4, end_span/2, get_trace/1, clear/0, generate_id/0]).
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

%% Gen_server callbacks
init([]) ->
    {ok, #{}}.

handle_call({start_span, Span}, _From, State) ->
    F = fun() -> mnesia:write(span, Span, write) end,
    mnesia:transaction(F),
    {reply, ok, State};

handle_call({end_span, TraceId, SpanId, EndTime}, _From, State) ->
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
    Res = case mnesia:transaction(F) of
        {atomic, Result} -> Result;
        {aborted, _Reason} -> {error, mnesia_error}
    end,
    {reply, Res, State};

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
    {reply, ok, State};

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
