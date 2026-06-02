-module(sofia_config).
-behaviour(gen_server).

%% API
-export([start_link/0, set/2, get/1, get/2, set_local/2]).

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
    %% Set locally first
    set_local(Key, Value),
    %% Broadcast to all other nodes in the Erlang cluster for federated configuration sync
    [rpc:cast(Node, ?MODULE, set_local, [Key, Value]) || Node <- nodes()],
    ok.

get(Key) ->
    get(Key, undefined).

get(Key, Default) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Value}] ->
            Value;
        [] ->
            Default
    end.

set_local(Key, Value) ->
    ets:insert(?TABLE, {Key, Value}),
    ok.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Public read concurrency table for fast, non-blocking configuration reads
    ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
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
