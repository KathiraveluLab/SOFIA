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
    %% Use QoS-aware router: picks the healthiest endpoint (best breaker state,
    %% shortest mailbox, lowest historical latency). Falls back to first-in-list.
    BestFirstFun = fun(_, [Best | _]) -> {ok, Best} end,
    case sofia_router:route(ServiceType, Request, BestFirstFun) of
        {error, Reason} ->
            {error, {service_discovery_failed, Reason}};
        {ok, ServicePid} ->
            MaxQueueLen = application:get_env(sofia, max_mailbox_size, 100),
            case erlang:process_info(ServicePid, message_queue_len) of
                {message_queue_len, Len} when Len > MaxQueueLen ->
                    {error, overloaded};
                undefined ->
                    {error, service_dead};
                _ ->
                    InvocationFun = fun() ->
                        gen_server:call(ServicePid, Request)
                    end,
                    BreakerId = list_to_atom(atom_to_list(ServiceType) ++ "_breaker"),
                    case sofia_registry:get_contract(ServiceType) of
                        {ok, Contract} ->
                            case Request of
                                {Method, Payload} when is_map(Payload) ->
                                    case sofia_contract:validate_request(Contract, Method, Payload) of
                                        ok ->
                                            sofia_breaker:call(BreakerId, InvocationFun, BreakerOpts);
                                        {error, ValReason} ->
                                            {error, {contract_validation_failed, ValReason}}
                                    end;
                                _ ->
                                    sofia_breaker:call(BreakerId, InvocationFun, BreakerOpts)
                            end;
                        {error, no_contract} ->
                            sofia_breaker:call(BreakerId, InvocationFun, BreakerOpts)
                    end
            end
    end.

%% @doc Example client function to ping a service type.
-spec ping_service(ServiceType :: atom()) -> pong | {error, Reason :: term()}.
ping_service(ServiceType) ->
    call_service(ServiceType, ping).
