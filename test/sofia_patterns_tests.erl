-module(sofia_patterns_tests).
-include_lib("eunit/include/eunit.hrl").

sofia_patterns_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_pubsub/0,
      fun test_pipeline/0,
      fun test_scatter_gather/0,
      fun test_roa/0,
      fun test_multitenant/0,
      fun test_sfc/0,
      fun test_workflow/0,
      fun test_orchestrator_sfc_success/0,
      fun test_orchestrator_sfc_saga/0,
      fun test_orchestrator_workflow_success/0,
      fun test_orchestrator_tracing/0
     ]}.

setup() ->
    case application:start(sasl) of
        ok -> ok;
        {error, {already_started, sasl}} -> ok
    end,
    {ok, Apps} = application:ensure_all_started(sofia),
    Apps.

cleanup(Apps) ->
    [application:stop(App) || App <- lists:reverse(Apps)],
    application:stop(sasl),
    ok.

test_pubsub() ->
    Self = self(),
    _Sub1 = spawn(fun() ->
        ok = sofia_pubsub:subscribe("market_feed"),
        receive
            {sofia_pubsub, "market_feed", Msg} -> Self ! {sub1, Msg}
        end
    end),
    _Sub2 = spawn(fun() ->
        ok = sofia_pubsub:subscribe("market_feed"),
        receive
            {sofia_pubsub, "market_feed", Msg} -> Self ! {sub2, Msg}
        end
    end),
    
    %% Allow registration to propagate
    timer:sleep(50),
    
    %% Publish message
    Count = sofia_pubsub:publish("market_feed", "hello_world"),
    ?assertEqual(2, Count),
    
    %% Assert both received it
    receive {sub1, Msg1} -> ?assertEqual("hello_world", Msg1) end,
    receive {sub2, Msg2} -> ?assertEqual("hello_world", Msg2) end,
    
    %% Unsubscribe test using current process
    ok = sofia_pubsub:subscribe("feed_test"),
    ?assertEqual(1, sofia_pubsub:publish("feed_test", "msg1")),
    receive
        {sofia_pubsub, "feed_test", "msg1"} -> ok
    after 100 ->
        ?assert(false)
    end,
    
    ok = sofia_pubsub:unsubscribe("feed_test"),
    ?assertEqual(0, sofia_pubsub:publish("feed_test", "msg2")),
    receive
        {sofia_pubsub, "feed_test", "msg2"} -> ?assert(false)
    after 100 ->
        ok
    end.

test_pipeline() ->
    Self = self(),
    %% Run workers in a recursive loop to handle multiple/random tasks
    Loop = fun L(Name) ->
        receive
            {sofia_pipeline_task, "job_pipeline", Task} ->
                Self ! {Name, Task},
                L(Name);
            stop -> ok
        end
    end,
    Worker1 = spawn(fun() -> ok = sofia_pipeline:register_worker("job_pipeline"), Loop(worker1) end),
    Worker2 = spawn(fun() -> ok = sofia_pipeline:register_worker("job_pipeline"), Loop(worker2) end),
    
    timer:sleep(50),
    
    %% Push two tasks
    ?assertEqual(ok, sofia_pipeline:push_task("job_pipeline", task1)),
    ?assertEqual(ok, sofia_pipeline:push_task("job_pipeline", task2)),
    
    %% Collect 2 results in any order due to randomized load balancing
    Results = collect_pipeline_results(2, []),
    ?assertEqual(2, length(Results)),
    
    %% Verify both tasks were processed
    Tasks = [T || {_, T} <- Results],
    ?assert(lists:member(task1, Tasks)),
    ?assert(lists:member(task2, Tasks)),
    
    %% Verify push returns error when no workers are left
    ?assertEqual({error, no_service_available}, sofia_pipeline:push_task("non_existent", task3)),
    
    %% Clean up
    sofia_pipeline:deregister_worker("job_pipeline"),
    Worker1 ! stop,
    Worker2 ! stop.

collect_pipeline_results(0, Acc) -> Acc;
collect_pipeline_results(N, Acc) ->
    receive
        {Worker, Task} -> collect_pipeline_results(N - 1, [{Worker, Task} | Acc])
    after 200 ->
        Acc
    end.

test_scatter_gather() ->
    %% Start 3 mock nodes that respond to scatter request with different prices/estimates
    Mock1 = spawn(fun() ->
        receive
            {scatter, Ref, From, {get_quote, Item}} ->
                From ! {gather, Ref, {mock1, Item, 100}}
        end
    end),
    Mock2 = spawn(fun() ->
        receive
            {scatter, Ref, From, {get_quote, Item}} ->
                From ! {gather, Ref, {mock2, Item, 120}}
        end
    end),
    Mock3 = spawn(fun() ->
        receive
            {scatter, Ref, From, {get_quote, Item}} ->
                From ! {gather, Ref, {mock3, Item, 110}}
        end
    end),

    ok = sofia_registry:register_service(quote_provider, Mock1),
    ok = sofia_registry:register_service(quote_provider, Mock2),
    ok = sofia_registry:register_service(quote_provider, Mock3),

    timer:sleep(50),

    %% Invoke scatter gather
    {ok, Replies} = sofia_scatter_gather:request(quote_provider, {get_quote, <<"widget">>}, 1000),
    
    %% Verify we received responses from all 3 providers
    ?assertEqual(3, length(Replies)),
    ?assert(lists:member({mock1, <<"widget">>, 100}, Replies)),
    ?assert(lists:member({mock2, <<"widget">>, 120}, Replies)),
    ?assert(lists:member({mock3, <<"widget">>, 110}, Replies)),

    %% Verify error returned when no providers are available
    ?assertEqual({error, no_service_available}, sofia_scatter_gather:request(non_existent_provider, {get_quote, <<"widget">>}, 1000)),

    %% Clean up registry
    ok = sofia_registry:deregister_service(quote_provider, Mock1),
    ok = sofia_registry:deregister_service(quote_provider, Mock2),
    ok = sofia_registry:deregister_service(quote_provider, Mock3).

test_roa() ->
    %% Start a mock resource actor representing a patient resource "/patients/101"
    Loop = fun L(State) ->
        receive
            {roa_request, Ref, From, get} ->
                From ! {roa_response, Ref, {ok, State}},
                L(State);
            {roa_request, Ref, From, {put, NewState}} ->
                From ! {roa_response, Ref, {ok, NewState}},
                L(NewState);
            {roa_request, Ref, From, {post, Data}} ->
                NewState = State ++ [Data],
                From ! {roa_response, Ref, {ok, NewState}},
                L(NewState);
            {roa_request, Ref, From, delete} ->
                From ! {roa_response, Ref, {ok, deleted}},
                ok;
            stop ->
                ok
        end
    end,
    
    ResourcePid = spawn(fun() -> Loop([]) end),
    ok = sofia_roa:register_resource("/patients/101", ResourcePid),
    
    timer:sleep(50),
    
    %% Verify GET returns empty state
    ?assertEqual({ok, []}, sofia_roa:get("/patients/101")),
    
    %% Verify PUT modifies state
    ?assertEqual({ok, ["John Doe"]}, sofia_roa:put("/patients/101", ["John Doe"])),
    ?assertEqual({ok, ["John Doe"]}, sofia_roa:get("/patients/101")),
    
    %% Verify POST appends to state
    ?assertEqual({ok, ["John Doe", "Active"]}, sofia_roa:post("/patients/101", "Active")),
    ?assertEqual({ok, ["John Doe", "Active"]}, sofia_roa:get("/patients/101")),
    
    %% Verify DELETE terminates resource
    ?assertEqual({ok, deleted}, sofia_roa:delete("/patients/101")),
    
    timer:sleep(50),
    
    %% Verify discovery fails now
    ?assertEqual({error, no_service_available}, sofia_roa:get("/patients/101")),
    
    %% Clean up (deregister)
    ok = sofia_roa:deregister_resource("/patients/101", ResourcePid).

test_multitenant() ->
    TenantAPid = spawn(fun() -> receive stop -> ok end end),
    GlobalPid = spawn(fun() -> receive stop -> ok end end),
    
    ok = sofia_multitenant:register_tenant_service(tenant_A, calc_service, TenantAPid),
    ok = sofia_multitenant:register_tenant_service(global, calc_service, GlobalPid),
    
    timer:sleep(50),
    
    %% Tenant A discovers Tenant A service
    ?assertEqual({ok, TenantAPid}, sofia_multitenant:discover_tenant_service(tenant_A, calc_service)),
    
    %% Tenant B falls back to global/shared service
    ?assertEqual({ok, GlobalPid}, sofia_multitenant:discover_tenant_service(tenant_B, calc_service)),
    
    %% Clean up
    TenantAPid ! stop,
    GlobalPid ! stop,
    ok = sofia_multitenant:deregister_tenant_service(tenant_A, calc_service, TenantAPid),
    ok = sofia_multitenant:deregister_tenant_service(global, calc_service, GlobalPid).

test_sfc() ->
    Self = self(),
    
    %% Create step processes that modify the payload and forward
    AuthPid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                sofia_sfc:forward_chain(Remaining, Payload ++ [auth], Originator),
                L();
            stop -> ok
        end
    end),
    
    ValidatePid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                sofia_sfc:forward_chain(Remaining, Payload ++ [valid], Originator),
                L();
            stop -> ok
        end
    end),
    
    LogPid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                sofia_sfc:forward_chain(Remaining, Payload ++ [logged], Originator),
                L();
            stop -> ok
        end
    end),
    
    ok = sofia_registry:register_service(auth_step, AuthPid),
    ok = sofia_registry:register_service(validate_step, ValidatePid),
    ok = sofia_registry:register_service(log_step, LogPid),
    
    timer:sleep(50),
    
    %% Start chain
    Chain = [auth_step, validate_step, log_step],
    ok = sofia_sfc:start_chain(Chain, [start], Self),
    
    %% Verify completion response
    receive
        {sfc_complete, FinalPayload} ->
            ?assertEqual([start, auth, valid, logged], FinalPayload)
    after 1000 ->
        ?assert(false)
    end,
    
    %% Clean up
    AuthPid ! stop,
    ValidatePid ! stop,
    LogPid ! stop,
    ok = sofia_registry:deregister_service(auth_step, AuthPid),
    ok = sofia_registry:deregister_service(validate_step, ValidatePid),
    ok = sofia_registry:deregister_service(log_step, LogPid).

test_workflow() ->
    %% YAML specification representing a directed hypergraph where auth fays out to validate and notify
    Yaml = "
name: hypergraph_workflow
- source: auth_node
  destinations:
    - validate_node
    - notify_node
- source: validate_node
  destinations:
    - billing_node
",
    {ok, Workflow} = sofia_workflow:parse_yaml(Yaml),
    ?assertEqual("hypergraph_workflow", maps:get(name, Workflow)),
    
    Edges = maps:get(edges, Workflow),
    ?assertEqual([validate_node, notify_node], maps:get(auth_node, Edges)),
    ?assertEqual([billing_node], maps:get(validate_node, Edges)),
    
    Self = self(),
    
    %% Spawn actor loops for each node in the hypergraph
    AuthPid = spawn(fun L() ->
        receive
            {workflow_step, Wf, Node, Payload, Ref, Orig} ->
                sofia_workflow:complete_step(Wf, Node, Payload ++ [auth], Ref, Orig),
                L();
            stop -> ok
        end
    end),
    
    ValidatePid = spawn(fun L() ->
        receive
            {workflow_step, Wf, Node, Payload, Ref, Orig} ->
                sofia_workflow:complete_step(Wf, Node, Payload ++ [valid], Ref, Orig),
                L();
            stop -> ok
        end
    end),
    
    NotifyPid = spawn(fun L() ->
        receive
            {workflow_step, Wf, Node, Payload, Ref, Orig} ->
                sofia_workflow:complete_step(Wf, Node, Payload ++ [notified], Ref, Orig),
                L();
            stop -> ok
        end
    end),
    
    BillingPid = spawn(fun L() ->
        receive
            {workflow_step, Wf, Node, Payload, Ref, Orig} ->
                sofia_workflow:complete_step(Wf, Node, Payload ++ [billed], Ref, Orig),
                L();
            stop -> ok
        end
    end),
    
    ok = sofia_registry:register_service(auth_node, AuthPid),
    ok = sofia_registry:register_service(validate_node, ValidatePid),
    ok = sofia_registry:register_service(notify_node, NotifyPid),
    ok = sofia_registry:register_service(billing_node, BillingPid),
    
    timer:sleep(50),
    
    %% Start workflow at auth_node
    ok = sofia_workflow:execute(Workflow, auth_node, [start], Self),
    
    Results = collect_wf_results(2, []),
    ?assertEqual(2, length(Results)),
    ?assert(lists:member({notify_node, [start, auth, notified]}, Results)),
    ?assert(lists:member({billing_node, [start, auth, valid, billed]}, Results)),
    
    %% Clean up
    AuthPid ! stop,
    ValidatePid ! stop,
    NotifyPid ! stop,
    BillingPid ! stop,
    ok = sofia_registry:deregister_service(auth_node, AuthPid),
    ok = sofia_registry:deregister_service(validate_node, ValidatePid),
    ok = sofia_registry:deregister_service(notify_node, NotifyPid),
    ok = sofia_registry:deregister_service(billing_node, BillingPid).

collect_wf_results(0, Acc) -> Acc;
collect_wf_results(N, Acc) ->
    receive
        {workflow_branch_complete, Node, Payload, _Ref} ->
            collect_wf_results(N - 1, [{Node, Payload} | Acc])
    after 1000 ->
        Acc
    end.

test_orchestrator_sfc_success() ->
    AuthPid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                sofia_sfc:forward_chain(Remaining, Payload ++ [auth], Originator),
                L();
            stop -> ok
        end
    end),
    ValidatePid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                sofia_sfc:forward_chain(Remaining, Payload ++ [valid], Originator),
                L();
            stop -> ok
        end
    end),
    
    ok = sofia_registry:register_service(auth_step, AuthPid),
    ok = sofia_registry:register_service(validate_step, ValidatePid),
    
    timer:sleep(50),
    
    %% Run SFC with Orchestrator (default retry policy)
    {ok, Result, _TraceId} = sofia_orchestrator:execute_sfc([auth_step, validate_step], [start], #{}),
    ?assertEqual([start, auth, valid], Result),
    
    AuthPid ! stop,
    ValidatePid ! stop,
    ok = sofia_registry:deregister_service(auth_step, AuthPid),
    ok = sofia_registry:deregister_service(validate_step, ValidatePid).

test_orchestrator_sfc_saga() ->
    Self = self(),
    
    AuthPid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                sofia_sfc:forward_chain(Remaining, Payload ++ [auth], Originator),
                L();
            {'$gen_call', From, {undo_auth, Payload}} ->
                Self ! {auth_compensated, Payload},
                gen_server:reply(From, ok),
                L();
            stop -> ok
        end
    end),
    
    %% Register service contract with compensation definition
    Contract = #{
        compensations => #{
            do_auth => undo_auth
        }
    },
    ok = sofia_registry:register_service(auth_step, AuthPid, Contract),
    
    timer:sleep(50),
    
    %% Run SFC with a non-existent second service to trigger failure
    Chain = [auth_step, non_existent_step],
    {error, {saga_rolled_back, {failed_at, non_existent_step, _Reason}}, _TraceId} = 
        sofia_orchestrator:execute_sfc(Chain, [start], #{policy => saga, timeout => 200}),
        
    %% Assert that compensation was triggered on auth_step
    receive
        {auth_compensated, Payload} ->
            ?assertEqual([start, auth], Payload)
    after 1000 ->
        ?assert(false)
    end,
    
    AuthPid ! stop,
    ok = sofia_registry:deregister_service(auth_step, AuthPid).

test_orchestrator_workflow_success() ->
    Yaml = "
name: orchestrated_workflow
- source: step_a
  destinations:
    - step_b
",
    {ok, Workflow} = sofia_workflow:parse_yaml(Yaml),
    
    PidA = spawn(fun L() ->
        receive
            {workflow_step, Wf, Node, Payload, Ref, Orig} ->
                sofia_workflow:complete_step(Wf, Node, Payload ++ [a], Ref, Orig),
                L();
            stop -> ok
        end
    end),
    PidB = spawn(fun L() ->
        receive
            {workflow_step, Wf, Node, Payload, Ref, Orig} ->
                sofia_workflow:complete_step(Wf, Node, Payload ++ [b], Ref, Orig),
                L();
            stop -> ok
        end
    end),
    
    ok = sofia_registry:register_service(step_a, PidA),
    ok = sofia_registry:register_service(step_b, PidB),
    
    timer:sleep(50),
    
    {ok, CompletedNodes, _TraceId} = sofia_orchestrator:execute_workflow(Workflow, step_a, [start]),
    
    %% Verify all nodes completed
    ?assertEqual(2, length(CompletedNodes)),
    ?assertEqual([start, a, b], proplists:get_value(step_b, CompletedNodes)),
    
    PidA ! stop,
    PidB ! stop,
    ok = sofia_registry:deregister_service(step_a, PidA),
    ok = sofia_registry:deregister_service(step_b, PidB).

test_orchestrator_tracing() ->
    ok = sofia_tracer:clear(),
    
    AuthPid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                timer:sleep(10), %% add measurable duration
                sofia_sfc:forward_chain(Remaining, Payload ++ [auth], Originator),
                L();
            stop -> ok
        end
    end),
    ValidatePid = spawn(fun L() ->
        receive
            {sfc_step, Remaining, Payload, Originator} ->
                timer:sleep(15), %% add measurable duration
                sofia_sfc:forward_chain(Remaining, Payload ++ [valid], Originator),
                L();
            stop -> ok
        end
    end),
    
    ok = sofia_registry:register_service(auth_step, AuthPid),
    ok = sofia_registry:register_service(validate_step, ValidatePid),
    
    timer:sleep(50),
    
    {ok, _Result, TraceId} = sofia_orchestrator:execute_sfc([auth_step, validate_step], [start], #{}),
    
    %% Retrieve spans from the tracer
    Spans = sofia_tracer:get_trace(TraceId),
    ?assertEqual(2, length(Spans)),
    
    [Span1, Span2] = Spans,
    ?assertEqual(auth_step, maps:get(name, Span1)),
    ?assertEqual(validate_step, maps:get(name, Span2)),
    
    %% Verify duration calculations
    ?assert(maps:get(duration, Span1) >= 10000), %% 10ms in microseconds
    ?assert(maps:get(duration, Span2) >= 15000), %% 15ms in microseconds
    
    AuthPid ! stop,
    ValidatePid ! stop,
    ok = sofia_registry:deregister_service(auth_step, AuthPid),
    ok = sofia_registry:deregister_service(validate_step, ValidatePid).

