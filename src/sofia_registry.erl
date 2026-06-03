-module(sofia_registry).
-behaviour(gen_server).

%% API
-export([start_link/0, register_service/2, register_service/3, deregister_service/2, discover/1, discover_all/1, get_contract/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(SCOPE, sofia_pg_scope).

-record(state, {
    contracts = #{} :: map()
}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_service(ServiceType, Pid) ->
    gen_server:call(?SERVER, {register, ServiceType, Pid}).

register_service(ServiceType, Pid, Contract) ->
    gen_server:call(?SERVER, {register, ServiceType, Pid, Contract}).

deregister_service(ServiceType, Pid) ->
    gen_server:call(?SERVER, {deregister, ServiceType, Pid}).

discover(ServiceType) ->
    case discover_all(ServiceType) of
        [] ->
            {error, no_service_available};
        Pids ->
            Idx = rand:uniform(length(Pids)),
            {ok, lists:nth(Idx, Pids)}
    end.

discover_all(ServiceType) ->
    pg:get_members(?SCOPE, ServiceType).

get_contract(ServiceType) ->
    gen_server:call(?SERVER, {get_contract, ServiceType}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Start the local pg scope for federated service discovery
    {ok, _PgPid} = pg:start_link(?SCOPE),
    {ok, #state{contracts = #{}}}.

handle_call({register, ServiceType, Pid}, _From, State) ->
    ok = pg:join(?SCOPE, ServiceType, Pid),
    {reply, ok, State};

handle_call({register, ServiceType, Pid, Contract}, _From, State) ->
    ok = pg:join(?SCOPE, ServiceType, Pid),
    NewContracts = maps:put(ServiceType, Contract, State#state.contracts),
    {reply, ok, State#state{contracts = NewContracts}};

handle_call({deregister, ServiceType, Pid}, _From, State) ->
    _ = pg:leave(?SCOPE, ServiceType, Pid),
    {reply, ok, State};

handle_call({get_contract, ServiceType}, _From, State) ->
    Reply = case maps:find(ServiceType, State#state.contracts) of
        {ok, Contract} -> {ok, Contract};
        error -> {error, no_contract}
    end,
    {reply, Reply, State};

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
