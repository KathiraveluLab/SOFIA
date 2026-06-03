-module(sofia_config).
-behaviour(gen_server).

%% API
-export([start_link/0, set/2, get/1, get/2, set_local/2, set_local/4, request_push/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, sofia_config_table).

-record(state, {}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

set(Key, Value) ->
    Timestamp = erlang:system_time(microsecond),
    Node = node(),
    set_local(Key, Value, Timestamp, Node),
    %% Broadcast to all other nodes in the Erlang cluster for federated configuration sync
    [rpc:cast(N, ?MODULE, set_local, [Key, Value, Timestamp, Node]) || N <- nodes()],
    ok.

get(Key) ->
    get(Key, undefined).

get(Key, Default) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Value, _Timestamp, _Node}] ->
            Value;
        [] ->
            Default
    end.

set_local(Key, Value) ->
    set_local(Key, Value, 0, node()).

set_local(Key, Value, Timestamp, Node) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, _, ExistingTimestamp, ExistingNode}] ->
            %% Last-Write-Wins (LWW) Register: compare timestamp first, tie-break on Node name
            case {Timestamp, Node} > {ExistingTimestamp, ExistingNode} of
                true ->
                    ets:insert(?TABLE, {Key, Value, Timestamp, Node}),
                    ok;
                false ->
                    ok
            end;
        [] ->
            ets:insert(?TABLE, {Key, Value, Timestamp, Node}),
            ok
    end.

%% Pushes our entire local config to target node
request_push(TargetNode) ->
    LocalEntries = ets:tab2list(?TABLE),
    [rpc:cast(TargetNode, ?MODULE, set_local, [K, V, T, N]) || {K, V, T, N} <- LocalEntries],
    ok.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Public read concurrency table for fast, non-blocking configuration reads
    ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    %% Monitor node changes to detect network partition healing and trigger self-healing syncs
    ok = net_kernel:monitor_nodes(true),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node}, State) ->
    %% Bi-directional reconciliation:
    %% 1. Send our config database to reconnecting node
    LocalEntries = ets:tab2list(?TABLE),
    [rpc:cast(Node, ?MODULE, set_local, [K, V, T, N]) || {K, V, T, N} <- LocalEntries],
    %% 2. Request their database pushed to us
    rpc:cast(Node, ?MODULE, request_push, [node()]),
    {noreply, State};
handle_info({nodedown, _Node}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
