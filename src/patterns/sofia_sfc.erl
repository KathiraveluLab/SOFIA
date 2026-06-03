-module(sofia_sfc).
-export([start_chain/3, forward_chain/3]).

%% @doc Initiates a service function chain execution with a given payload and originator process.
start_chain(Chain, Payload, Originator) ->
    forward_chain(Chain, Payload, Originator).

%% @doc Forwards the payload and chain state to the next service.
%% If the chain is completed, it returns the final response back to the originator.
forward_chain([], Payload, Originator) ->
    Originator ! {sfc_complete, Payload},
    ok;
forward_chain([NextService | RemainingChain], Payload, Originator) ->
    Originator ! {sfc_progress, NextService, Payload},
    case sofia_registry:discover(NextService) of
        {ok, ServicePid} ->
            ServicePid ! {sfc_step, RemainingChain, Payload, Originator},
            ok;
        {error, Reason} ->
            Originator ! {sfc_error, {NextService, Reason}},
            {error, {discovery_failed, NextService, Reason}}
    end.
