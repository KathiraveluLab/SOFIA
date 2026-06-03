%%%-------------------------------------------------------------------
%%% @doc
%%% SOFIA Service Skeleton
%%%
%%% This module provides a template/skeleton for implementing federated
%%% services within the SOFIA framework. Developers should copy this file,
%%% rename the module (e.g., my_custom_service), define their service type,
%%% and implement their specific business logic.
%%%
%%% When started, this service automatically registers itself with the
%%% `sofia_registry`. Upon termination, it automatically deregisters.
%%% @end
%%%-------------------------------------------------------------------
-module(sofia_service_skeleton).
-behaviour(gen_server).

%% API
-export([start_link/1, stop/1]).
-export([ping/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    service_type :: atom(),
    custom_state :: term()
}).

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc Starts the service and registers it under the provided ServiceType.
-spec start_link(ServiceType :: atom()) -> {ok, pid()} | {error, term()}.
start_link(ServiceType) ->
    gen_server:start_link(?MODULE, [ServiceType], []).

%% @doc Stops the service.
-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Example service API function (ping).
-spec ping(Pid :: pid()) -> pong.
ping(Pid) ->
    gen_server:call(Pid, ping).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @private
init([ServiceType]) ->
    %% Register this service process under the specified service type in the federated registry
    ok = sofia_registry:register_service(ServiceType, self()),
    {ok, #state{service_type = ServiceType, custom_state = undefined}}.

%% @private
handle_call(ping, _From, State) ->
    {reply, pong, State};

%% TODO: Implement your service's call handlers here
handle_call({custom_request, _RequestPayload}, _From, State) ->
    %% Process request...
    Reply = {ok, processed_payload},
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, State) ->
    %% Clean up registration from the federated registry upon termination
    ok = sofia_registry:deregister_service(State#state.service_type, self()),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
