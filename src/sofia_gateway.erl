-module(sofia_gateway).
-behaviour(gen_server).

%% API
-export([start_link/0, handle_request/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Simulates receiving an external protocol request (e.g. REST/JSON mapped to an Erlang map)
%% and bridging/routing it natively via sofia_breaker and sofia_registry.
handle_request(ServiceType, ExternalPayload, BreakerId) ->
    case sofia_registry:discover(ServiceType) of
        {ok, Pid} ->
            %% Perform protocol translation / bridging: convert Map payload to native Erlang record or tuple
            NativeMsg = translate_payload(ExternalPayload),
            %% Call with circuit breaker protection
            sofia_breaker:call(BreakerId, fun() -> gen_server:call(Pid, NativeMsg) end);
        {error, Reason} ->
            {error, {gateway_discovery_failed, Reason}}
    end.

%% ===================================================================
%% Internal Helpers
%% ===================================================================

translate_payload(#{<<"action">> := <<"add">>, <<"args">> := [A, B]}) ->
    {add, A, B};
translate_payload(#{<<"action">> := <<"subtract">>, <<"args">> := [A, B]}) ->
    {subtract, A, B};
translate_payload(#{<<"action">> := <<"payment_gateway_charge">>,
                    <<"patient_id">> := PatientId,
                    <<"billing_cents">> := Cents,
                    <<"method">> := Method}) ->
    {payment_gateway_charge, PatientId, Cents, Method};
translate_payload(Payload) ->
    %% Fallback mapping
    {raw_request, Payload}.

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
