-module(sofia_tracer).
-behaviour(gen_server).

-export([start_link/0, start_span/3, start_span/4, end_span/2, get_trace/1, clear/0, generate_id/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(span, {
    trace_id :: binary(),
    span_id :: binary(),
    parent_span_id :: binary() | undefined,
    name :: atom() | string(),
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
        trace_id = TraceId,
        span_id = SpanId,
        parent_span_id = ParentSpanId,
        name = Name,
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
    ets:new(sofia_spans, [set, named_table, {keypos, 3}, public]),
    %% Note: span_id is the unique keypos (field 3 of #span is span_id)
    {ok, #{}}.

handle_call({start_span, Span}, _From, State) ->
    ets:insert(sofia_spans, Span),
    {reply, ok, State};

handle_call({end_span, TraceId, SpanId, EndTime}, _From, State) ->
    case ets:lookup(sofia_spans, SpanId) of
        [Span] when Span#span.trace_id =:= TraceId ->
            Duration = EndTime - Span#span.start_time,
            UpdatedSpan = Span#span{end_time = EndTime, duration = Duration},
            ets:insert(sofia_spans, UpdatedSpan),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_trace, TraceId}, _From, State) ->
    Spans = ets:select(sofia_spans, [{
        #span{trace_id = TraceId, _ = '_'},
        [],
        ['$_']
    }]),
    Sorted = lists:keysort(#span.start_time, Spans),
    MapSpans = [#{
        trace_id => S#span.trace_id,
        span_id => S#span.span_id,
        parent_span_id => S#span.parent_span_id,
        name => S#span.name,
        start_time => S#span.start_time,
        end_time => S#span.end_time,
        duration => S#span.duration
    } || S <- Sorted],
    {reply, MapSpans, State};

handle_call(clear, _From, State) ->
    ets:delete_all_objects(sofia_spans),
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
