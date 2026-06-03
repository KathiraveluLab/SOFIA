-module(sofia_router).
-behaviour(gen_server).

%% API
-export([start_link/0, route/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

-record(span, {
    span_id,
    trace_id,
    parent_span_id,
    name,
    node,
    start_time,
    end_time,
    duration
}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Dynamically route payload to a specific service instance using QoS-aware ranking and a routing function
route(ServiceType, Payload, RoutingKeyFun) ->
    case sofia_registry:discover_all(ServiceType) of
        [] ->
            {error, no_service_available};
        Pids ->
            MaxQueueLen = application:get_env(sofia, max_mailbox_size, 100),
            HealthyPids = lists:filter(fun(Pid) ->
                case erlang:process_info(Pid, message_queue_len) of
                    {message_queue_len, Len} when Len > MaxQueueLen -> false;
                    undefined -> false;
                    _ -> true
                end
            end, Pids),
            %% Apply QoS sorting and filtering (based on breaker state, mailbox length, and tracing latency)
            case sort_by_qos(ServiceType, HealthyPids) of
                [] ->
                    {error, overloaded};
                QoSSortedPids ->
                    %% Apply the dynamic routing criteria to choose a specific Pid from the sorted list
                    case RoutingKeyFun(Payload, QoSSortedPids) of
                        {ok, SelectedPid} ->
                            {ok, SelectedPid};
                        {error, Reason} ->
                            {error, {routing_failed, Reason}}
                    end
            end
    end.

%% ===================================================================
%% QoS Evaluation & Sorting
%% ===================================================================

sort_by_qos(ServiceType, Pids) ->
    PidsWithMetrics = [
        begin
            Node = node(Pid),
            MailboxLen = case erlang:process_info(Pid, message_queue_len) of
                {message_queue_len, L} -> L;
                undefined -> 9999
            end,
            BreakerState = case rpc:call(Node, sofia_breaker, get_state, [ServiceType]) of
                {ok, State} -> State;
                _ -> closed
            end,
            AvgLatency = get_average_latency(ServiceType, Node),
            {Pid, BreakerState, MailboxLen, AvgLatency}
        end || Pid <- Pids
    ],
    %% Filter out any Pid where the node's circuit is open
    ActivePids = lists:filter(fun({_, State, _, _}) -> State =/= open end, PidsWithMetrics),
    %% Lexicographical sort by:
    %% 1. Breaker state value (closed < half_open)
    %% 2. Mailbox queue length (shorter is better)
    %% 3. Average execution latency (lower is better)
    Sorted = lists:sort(fun({_, StateA, LenA, LatA}, {_, StateB, LenB, LatB}) ->
        ValA = {breaker_val(StateA), LenA, LatA},
        ValB = {breaker_val(StateB), LenB, LatB},
        ValA < ValB
    end, ActivePids),
    [Pid || {Pid, _, _, _} <- Sorted].

breaker_val(closed) -> 1;
breaker_val(half_open) -> 2;
breaker_val(open) -> 3.

get_average_latency(ServiceType, Node) ->
    F = fun() ->
        Pattern = #span{name = ServiceType, node = Node, _ = '_'},
        mnesia:match_object(Pattern)
    end,
    case mnesia:transaction(F) of
        {atomic, Spans} ->
            Durations = [D || #span{duration = D} <- Spans, D =/= undefined],
            case Durations of
                [] -> 0;
                _ -> lists:sum(Durations) div length(Durations)
            end;
        _ ->
            0
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
