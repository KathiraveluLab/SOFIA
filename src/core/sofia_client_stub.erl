%%%-------------------------------------------------------------------
%%% @doc
%%% SOFIA Client Stub
%%%
%%% This module provides a template/stub for implementing service clients
%%% in the SOFIA framework. Developers can copy and customize this stub
%%% to interact with specific services, wrapping calls in circuit breaker
%%% protection (`sofia_breaker`) and performing any necessary schema
%%% transformations.
%%% @end
%%%-------------------------------------------------------------------
-module(sofia_client_stub).

%% API
-export([call_service/2, call_service/3]).
-export([ping_service/1]).

-define(DEFAULT_MAX_FAILURES, 3).
-define(DEFAULT_RESET_TIMEOUT, 5000). %% 5 seconds in ms

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc Invokes a service using service discovery and circuit breaker protection.
%% Uses default circuit breaker options.
-spec call_service(ServiceType :: atom(), Request :: term()) -> {ok, Reply :: term()} | {error, Reason :: term()}.
call_service(ServiceType, Request) ->
    call_service(ServiceType, Request, #{}).

%% @doc Invokes a service with custom circuit breaker options.
-spec call_service(ServiceType :: atom(), Request :: term(), BreakerOpts :: map()) -> {ok, Reply :: term()} | {error, Reason :: term()}.
call_service(ServiceType, Request, BreakerOpts) ->
    %% 1. Discover a service instance from the federated registry
    case sofia_registry:discover(ServiceType) of
        {error, Reason} ->
            {error, {service_discovery_failed, Reason}};
        {ok, ServicePid} ->
            %% 2. Define the invocation function
            InvocationFun = fun() ->
                gen_server:call(ServicePid, Request)
            end,
            
            %% 3. Generate a circuit breaker ID specific to the service type
            BreakerId = list_to_atom(atom_to_list(ServiceType) ++ "_breaker"),
            
            %% 4. Invoke the service through the circuit breaker
            %% This protects the caller if the target process crashes or hangs.
            sofia_breaker:call(BreakerId, InvocationFun, BreakerOpts)
    end.

%% @doc Example client function to ping a service type.
-spec ping_service(ServiceType :: atom()) -> pong | {error, Reason :: term()}.
ping_service(ServiceType) ->
    call_service(ServiceType, ping).
