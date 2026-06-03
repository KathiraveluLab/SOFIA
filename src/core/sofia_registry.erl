-module(sofia_registry).
-behaviour(gen_server).

%% API
-export([start_link/0, register_service/2, register_service/3, register_service/4,
         deregister_service/2, discover/1, discover_all/1, discover_by_metadata/2, get_contract/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(SCOPE, sofia_pg_scope).

-record(state, {
    contracts = #{} :: map(),
    monitors = #{} :: map() %% Ref -> {ServiceType, Pid}
}).

-record(sofia_service_metadata, {
    pid,
    service_type,
    metadata = #{}
}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_service(ServiceType, Pid) ->
    register_service(ServiceType, Pid, #{}, #{}).

register_service(ServiceType, Pid, Contract) ->
    register_service(ServiceType, Pid, Contract, #{}).

register_service(ServiceType, Pid, Contract, Metadata) ->
    gen_server:call(?SERVER, {register, ServiceType, Pid, Contract, Metadata}).

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

discover_by_metadata(ServiceType, QueryMetadata) ->
    F = fun() ->
        Pattern = #sofia_service_metadata{service_type = ServiceType, _ = '_'},
        mnesia:match_object(Pattern)
    end,
    case mnesia:transaction(F) of
        {atomic, Records} ->
            MatchingPids = [
                Pid || #sofia_service_metadata{pid = Pid, metadata = Meta} <- Records,
                match_metadata(QueryMetadata, Meta)
            ],
            case MatchingPids of
                [] -> {error, no_service_available};
                _ ->
                    Idx = rand:uniform(length(MatchingPids)),
                    {ok, lists:nth(Idx, MatchingPids)}
            end;
        _ ->
            {error, no_service_available}
    end.

get_contract(ServiceType) ->
    gen_server:call(?SERVER, {get_contract, ServiceType}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Start the local pg scope for federated service discovery
    {ok, _PgPid} = pg:start_link(?SCOPE),
    {ok, #state{contracts = #{}, monitors = #{}}}.

handle_call({register, ServiceType, Pid, Contract, Metadata}, _From, State) ->
    ok = pg:join(?SCOPE, ServiceType, Pid),
    
    %% Write to local/distributed Mnesia metadata table
    F = fun() ->
        Record = #sofia_service_metadata{pid = Pid, service_type = ServiceType, metadata = Metadata},
        mnesia:write(sofia_service_metadata, Record, write)
    end,
    mnesia:transaction(F),
    
    %% Set up process monitor for auto-deregistration on process exit
    Ref = erlang:monitor(process, Pid),
    NewMonitors = maps:put(Ref, {ServiceType, Pid}, State#state.monitors),
    NewContracts = maps:put(ServiceType, Contract, State#state.contracts),
    
    {reply, ok, State#state{contracts = NewContracts, monitors = NewMonitors}};

handle_call({deregister, ServiceType, Pid}, _From, State) ->
    _ = pg:leave(?SCOPE, ServiceType, Pid),
    
    %% Delete from Mnesia metadata table
    F = fun() -> mnesia:delete({sofia_service_metadata, Pid}) end,
    mnesia:transaction(F),
    
    %% Clean up monitor if exists
    NewMonitors = case find_monitor_ref(Pid, State#state.monitors) of
        {ok, Ref} ->
            erlang:demonitor(Ref, [flush]),
            maps:remove(Ref, State#state.monitors);
        error ->
            State#state.monitors
    end,
    {reply, ok, State#state{monitors = NewMonitors}};

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

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:find(Ref, State#state.monitors) of
        {ok, {ServiceType, Pid}} ->
            %% Self-healing auto-deregistration
            _ = pg:leave(?SCOPE, ServiceType, Pid),
            F = fun() -> mnesia:delete({sofia_service_metadata, Pid}) end,
            mnesia:transaction(F),
            NewMonitors = maps:remove(Ref, State#state.monitors),
            {noreply, State#state{monitors = NewMonitors}};
        _ ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal Helpers
%% ===================================================================

match_metadata(Query, Meta) when is_map(Query), is_map(Meta) ->
    maps:fold(fun(K, V, Acc) ->
        Acc andalso (maps:get(K, Meta, undefined) =:= V)
    end, true, Query);
match_metadata(_, _) ->
    false.

find_monitor_ref(Pid, Monitors) ->
    maps:fold(fun(Ref, {_, P}, Acc) ->
        case P =:= Pid of
            true -> {ok, Ref};
            false -> Acc
        end
    end, error, Monitors).
