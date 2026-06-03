-module(sofia_orchestrator).
-export([execute_sfc/3, execute_workflow/3]).

%% ===================================================================
%% API Functions
%% ===================================================================

%% @doc Executes a Service Function Chain with monitoring and recovery.
execute_sfc(Chain, Payload, Options) ->
    Timeout = maps:get(timeout, Options, 5000),
    Policy = maps:get(policy, Options, retry),
    Self = self(),
    CoordinatorPid = spawn(fun() ->
        sfc_coordinator(Chain, Payload, Policy, Timeout, Self, [], undefined)
    end),
    ok = sofia_sfc:start_chain(Chain, Payload, CoordinatorPid),
    receive
        {sfc_complete, FinalResult} -> {ok, FinalResult};
        {sfc_failed, Error} -> {error, Error}
    after Timeout + 1000 ->
        {error, timeout}
    end.

%% @doc Executes a Directed Hypergraph Workflow with monitoring and recovery.
execute_workflow(Workflow, StartNode, Payload) ->
    Timeout = 5000,
    Self = self(),
    Edges = maps:get(edges, Workflow, #{}),
    ExpectedLeaves = find_leaves(StartNode, Edges),
    CoordinatorPid = spawn(fun() ->
        workflow_coordinator(Workflow, StartNode, Payload, Timeout, Self, ExpectedLeaves, [], [])
    end),
    ok = sofia_workflow:execute(Workflow, StartNode, Payload, CoordinatorPid),
    receive
        {workflow_complete, Completed} -> {ok, Completed};
        {workflow_failed, Error} -> {error, Error}
    after Timeout + 1000 ->
        {error, timeout}
    end.

%% ===================================================================
%% SFC Coordinator Loop
%% ===================================================================

sfc_coordinator(Chain, InitialPayload, Policy, Timeout, ParentPid, CompletedSteps, CurrentService) ->
    receive
        {sfc_progress, Service, Payload} ->
            NewCompleted = case CurrentService of
                undefined -> CompletedSteps;
                PrevService -> [{PrevService, Payload} | CompletedSteps]
            end,
            sfc_coordinator(Chain, InitialPayload, Policy, Timeout, ParentPid, NewCompleted, Service);
        
        {sfc_complete, FinalPayload} ->
            _NewCompleted = case CurrentService of
                undefined -> CompletedSteps;
                LastService -> [{LastService, FinalPayload} | CompletedSteps]
            end,
            ParentPid ! {sfc_complete, FinalPayload},
            ok;
            
        {sfc_error, {FailedService, Reason}} ->
            handle_sfc_failure(FailedService, Reason, Policy, CompletedSteps, ParentPid)
            
    after Timeout ->
        handle_sfc_failure(CurrentService, timeout, Policy, CompletedSteps, ParentPid)
    end.

handle_sfc_failure(FailedService, Reason, retry, _CompletedSteps, ParentPid) ->
    case FailedService of
        undefined ->
            ParentPid ! {sfc_failed, {timeout, no_service_active}},
            ok;
        _ ->
            case sofia_registry:discover(FailedService) of
                {ok, _NewPid} ->
                    ParentPid ! {sfc_failed, {failed_at, FailedService, Reason, retried}};
                {error, _} ->
                    ParentPid ! {sfc_failed, {failed_at, FailedService, Reason, no_failover_instance}}
            end
    end;
handle_sfc_failure(FailedService, Reason, saga, CompletedSteps, ParentPid) ->
    rollback_saga(CompletedSteps),
    ParentPid ! {sfc_failed, {saga_rolled_back, {failed_at, FailedService, Reason}}};
handle_sfc_failure(FailedService, Reason, _Other, _CompletedSteps, ParentPid) ->
    ParentPid ! {sfc_failed, {failed_at, FailedService, Reason}}.

rollback_saga([]) ->
    ok;
rollback_saga([{Service, Payload} | Rest]) ->
    case sofia_registry:get_contract(Service) of
        {ok, #{compensations := Compensations}} when is_map(Compensations) ->
            maps:fold(fun(_Method, CompMethod, _) ->
                sofia_client_stub:call_service(Service, {CompMethod, Payload})
            end, ok, Compensations);
        _ ->
            ok
    end,
    rollback_saga(Rest).

%% ===================================================================
%% Workflow Coordinator Loop
%% ===================================================================

workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, CompletedLeaves, CompletedNodes) ->
    receive
        {workflow_progress, _Node, _Payload, _Ref} ->
            workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, CompletedLeaves, CompletedNodes);
            
        {workflow_step_complete, Node, NewPayload, _Ref} ->
            NewCompleted = [{Node, NewPayload} | lists:keydelete(Node, 1, CompletedNodes)],
            workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, CompletedLeaves, NewCompleted);
            
        {workflow_branch_complete, Node, NewPayload, _Ref} ->
            NewCompleted = [{Node, NewPayload} | lists:keydelete(Node, 1, CompletedNodes)],
            NewCompletedLeaves = case lists:member(Node, CompletedLeaves) of
                true -> CompletedLeaves;
                false -> [Node | CompletedLeaves]
            end,
            case lists:sort(NewCompletedLeaves) =:= lists:sort(ExpectedLeaves) of
                true ->
                    ParentPid ! {workflow_complete, NewCompleted},
                    ok;
                false ->
                    workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, NewCompletedLeaves, NewCompleted)
            end;
            
        {workflow_error, Node, Reason} ->
            rollback_saga(CompletedNodes),
            ParentPid ! {workflow_failed, {failed_at, Node, Reason}}
            
    after Timeout ->
        rollback_saga(CompletedNodes),
        ParentPid ! {workflow_failed, timeout}
    end.

%% ===================================================================
%% Reachability Helper
%% ===================================================================

find_leaves(StartNode, Edges) ->
    find_leaves([StartNode], Edges, [], []).

find_leaves([], _Edges, _Visited, Leaves) ->
    Leaves;
find_leaves([Node | Rest], Edges, Visited, Leaves) ->
    case lists:member(Node, Visited) of
        true ->
            find_leaves(Rest, Edges, Visited, Leaves);
        false ->
            NewVisited = [Node | Visited],
            case maps:get(Node, Edges, []) of
                [] ->
                    find_leaves(Rest, Edges, NewVisited, [Node | Leaves]);
                Dests ->
                    find_leaves(Dests ++ Rest, Edges, NewVisited, Leaves)
            end
    end.
