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
    TraceId = maps:get(trace_id, Options, sofia_tracer:generate_id()),
    ParentSpanId = maps:get(parent_span_id, Options, undefined),
    CoordinatorPid = spawn(fun() ->
        sfc_coordinator(Chain, Payload, Policy, Timeout, Self, [], undefined, TraceId, ParentSpanId, undefined)
    end),
    ok = sofia_sfc:start_chain(Chain, Payload, CoordinatorPid),
    receive
        {sfc_complete, FinalResult, TraceId} -> {ok, FinalResult, TraceId};
        {sfc_failed, Error, TraceId} -> {error, Error, TraceId}
    after Timeout + 1000 ->
        {error, timeout}
    end.

%% @doc Executes a Directed Hypergraph Workflow with monitoring and recovery.
execute_workflow(Workflow, StartNode, Payload) ->
    Timeout = 5000,
    Self = self(),
    Edges = maps:get(edges, Workflow, #{}),
    ExpectedLeaves = find_leaves(StartNode, Edges),
    TraceId = sofia_tracer:generate_id(),
    CoordinatorPid = spawn(fun() ->
        workflow_coordinator(Workflow, StartNode, Payload, Timeout, Self, ExpectedLeaves, [], [], TraceId, #{}, #{})
    end),
    ok = sofia_workflow:execute(Workflow, StartNode, Payload, CoordinatorPid),
    receive
        {workflow_complete, Completed, TraceId} -> {ok, Completed, TraceId};
        {workflow_failed, Error, TraceId} -> {error, Error, TraceId}
    after Timeout + 1000 ->
        {error, timeout}
    end.

%% ===================================================================
%% SFC Coordinator Loop
%% ===================================================================

sfc_coordinator(Chain, InitialPayload, Policy, Timeout, ParentPid, CompletedSteps, CurrentService, TraceId, ParentSpanId, ActiveSpanId) ->
    receive
        {sfc_progress, Service, Payload} ->
            %% End active span for previous service
            case ActiveSpanId of
                undefined -> ok;
                _ -> sofia_tracer:end_span(TraceId, ActiveSpanId)
            end,
            %% Start new span for current service
            {ok, NewSpanId} = sofia_tracer:start_span(TraceId, Service, ParentSpanId),
            NewCompleted = case CurrentService of
                undefined -> CompletedSteps;
                PrevService -> [{PrevService, Payload} | CompletedSteps]
            end,
            sfc_coordinator(Chain, InitialPayload, Policy, Timeout, ParentPid, NewCompleted, Service, TraceId, ParentSpanId, NewSpanId);
        
        {sfc_complete, FinalPayload} ->
            case ActiveSpanId of
                undefined -> ok;
                _ -> sofia_tracer:end_span(TraceId, ActiveSpanId)
            end,
            ParentPid ! {sfc_complete, FinalPayload, TraceId},
            ok;
            
        {sfc_error, {FailedService, Reason}} ->
            case ActiveSpanId of
                undefined -> ok;
                _ -> sofia_tracer:end_span(TraceId, ActiveSpanId)
            end,
            handle_sfc_failure(FailedService, Reason, Policy, CompletedSteps, ParentPid, TraceId)
            
    after Timeout ->
        case ActiveSpanId of
            undefined -> ok;
            _ -> sofia_tracer:end_span(TraceId, ActiveSpanId)
        end,
        handle_sfc_failure(CurrentService, timeout, Policy, CompletedSteps, ParentPid, TraceId)
    end.

handle_sfc_failure(FailedService, Reason, retry, _CompletedSteps, ParentPid, TraceId) ->
    case FailedService of
        undefined ->
            ParentPid ! {sfc_failed, {timeout, no_service_active}, TraceId},
            ok;
        _ ->
            case sofia_registry:discover(FailedService) of
                {ok, _NewPid} ->
                    ParentPid ! {sfc_failed, {failed_at, FailedService, Reason, retried}, TraceId};
                {error, _} ->
                    ParentPid ! {sfc_failed, {failed_at, FailedService, Reason, no_failover_instance}, TraceId}
            end
    end;
handle_sfc_failure(FailedService, Reason, saga, CompletedSteps, ParentPid, TraceId) ->
    rollback_saga(CompletedSteps),
    ParentPid ! {sfc_failed, {saga_rolled_back, {failed_at, FailedService, Reason}}, TraceId};
handle_sfc_failure(FailedService, Reason, _Other, _CompletedSteps, ParentPid, TraceId) ->
    ParentPid ! {sfc_failed, {failed_at, FailedService, Reason}, TraceId}.

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

workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, CompletedLeaves, CompletedNodes, TraceId, ActiveSpans, SpanIds) ->
    receive
        {workflow_progress, Node, _Payload, _Ref, ParentNode} ->
            ParentSpanId = maps:get(ParentNode, SpanIds, undefined),
            {ok, SpanId} = sofia_tracer:start_span(TraceId, Node, ParentSpanId),
            NewActiveSpans = maps:put(Node, SpanId, ActiveSpans),
            NewSpanIds = maps:put(Node, SpanId, SpanIds),
            workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, CompletedLeaves, CompletedNodes, TraceId, NewActiveSpans, NewSpanIds);
            
        {workflow_step_complete, Node, NewPayload, _Ref} ->
            case maps:find(Node, ActiveSpans) of
                {ok, SpanId} -> sofia_tracer:end_span(TraceId, SpanId);
                error -> ok
            end,
            NewActiveSpans = maps:remove(Node, ActiveSpans),
            NewCompleted = [{Node, NewPayload} | lists:keydelete(Node, 1, CompletedNodes)],
            workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, CompletedLeaves, NewCompleted, TraceId, NewActiveSpans, SpanIds);
            
        {workflow_branch_complete, Node, NewPayload, _Ref} ->
            case maps:find(Node, ActiveSpans) of
                {ok, SpanId} -> sofia_tracer:end_span(TraceId, SpanId);
                error -> ok
            end,
            NewActiveSpans = maps:remove(Node, ActiveSpans),
            NewCompleted = [{Node, NewPayload} | lists:keydelete(Node, 1, CompletedNodes)],
            NewCompletedLeaves = case lists:member(Node, CompletedLeaves) of
                true -> CompletedLeaves;
                false -> [Node | CompletedLeaves]
            end,
            case lists:sort(NewCompletedLeaves) =:= lists:sort(ExpectedLeaves) of
                true ->
                    ParentPid ! {workflow_complete, NewCompleted, TraceId},
                    ok;
                false ->
                    workflow_coordinator(Workflow, StartNode, Payload, Timeout, ParentPid, ExpectedLeaves, NewCompletedLeaves, NewCompleted, TraceId, NewActiveSpans, SpanIds)
            end;
            
        {workflow_error, Node, Reason} ->
            %% End remaining spans
            maps:fold(fun(_N, SId, _) -> sofia_tracer:end_span(TraceId, SId) end, ok, ActiveSpans),
            rollback_saga(CompletedNodes),
            ParentPid ! {workflow_failed, {failed_at, Node, Reason}, TraceId}

    after Timeout ->
        maps:fold(fun(_N, SId, _) -> sofia_tracer:end_span(TraceId, SId) end, ok, ActiveSpans),
        rollback_saga(CompletedNodes),
        ParentPid ! {workflow_failed, timeout, TraceId}
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
