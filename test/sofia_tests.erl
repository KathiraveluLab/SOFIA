-module(sofia_tests).
-include_lib("eunit/include/eunit.hrl").

sofia_full_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_registry/0,
      fun test_breaker/0,
      fun test_config/0,
      fun test_gateway/0,
      fun test_router/0,
      fun test_transformer/0,
      fun test_saga/0,
      fun test_skeleton_and_stub/0,
      fun test_healthcare_finance_interconnection/0,
      fun test_pubsub/0,
      fun test_pipeline/0
     ]}.

setup() ->
    %% Start the application dependencies and application itself
    ok = application:start(sasl),
    {ok, Apps} = application:ensure_all_started(sofia),
    Apps.

cleanup(Apps) ->
    [application:stop(App) || App <- lists:reverse(Apps)],
    application:stop(sasl),
    ok.

test_registry() ->
    %% Verify discovery when no service is registered
    ?assertEqual({error, no_service_available}, sofia_registry:discover(my_dummy_service)),
    
    %% Register a dummy service Pid
    DummyPid = spawn(fun() -> receive {ping, From} -> From ! pong end end),
    ?assertEqual(ok, sofia_registry:register_service(my_dummy_service, DummyPid)),
    
    %% Verify discovery returns the registered Pid
    ?assertEqual({ok, DummyPid}, sofia_registry:discover(my_dummy_service)),
    ?assertEqual([DummyPid], sofia_registry:discover_all(my_dummy_service)),
    
    %% Deregister service
    ?assertEqual(ok, sofia_registry:deregister_service(my_dummy_service, DummyPid)),
    ?assertEqual({error, no_service_available}, sofia_registry:discover(my_dummy_service)).

test_breaker() ->
    Service = test_service,
    %% Default state is closed
    ?assertEqual({ok, closed}, sofia_breaker:get_state(Service)),
    
    %% Successful calls go through and keep breaker closed
    SuccessFun = fun() -> {ok, success_data} end,
    ?assertEqual({ok, success_data}, sofia_breaker:call(Service, SuccessFun)),
    ?assertEqual({ok, closed}, sofia_breaker:get_state(Service)),
    
    %% Failed calls increment failure count
    FailedFun = fun() -> {error, simulated_failure} end,
    ?assertEqual({error, simulated_failure}, sofia_breaker:call(Service, FailedFun, #{max_failures => 2})),
    ?assertEqual({ok, closed}, sofia_breaker:get_state(Service)),
    
    %% Second failure should trip the breaker to open
    ?assertEqual({error, simulated_failure}, sofia_breaker:call(Service, FailedFun, #{max_failures => 2})),
    ?assertEqual({ok, open}, sofia_breaker:get_state(Service)),
    
    %% Subsequent calls should fail immediately with circuit_open
    ?assertEqual({error, circuit_open}, sofia_breaker:call(Service, SuccessFun, #{max_failures => 2})),
    
    %% Reset the breaker manually
    ?assertEqual(ok, sofia_breaker:reset(Service)),
    ?assertEqual({ok, closed}, sofia_breaker:get_state(Service)),
    ?assertEqual({ok, success_data}, sofia_breaker:call(Service, SuccessFun)).

test_config() ->
    %% Unset key returns default
    ?assertEqual(undefined, sofia_config:get(non_existent_key)),
    ?assertEqual(my_default, sofia_config:get(non_existent_key, my_default)),
    
    %% Setting key updates it locally
    ?assertEqual(ok, sofia_config:set(my_key, "some_value")),
    ?assertEqual("some_value", sofia_config:get(my_key)),
    
    %% Local updates via set_local work
    ?assertEqual(ok, sofia_config:set_local(my_key2, 42)),
    ?assertEqual(42, sofia_config:get(my_key2)).

test_gateway() ->
    %% Start a mock server to handle translated requests
    MockServer = spawn(fun() ->
        receive
            {'$gen_call', From, {add, A, B}} ->
                gen_server:reply(From, {ok, A + B});
            {'$gen_call', From, _Other} ->
                gen_server:reply(From, {error, unknown})
        end
    end),
    ok = sofia_registry:register_service(mock_gateway_service, MockServer),
    
    %% Call gateway with external REST/JSON-like map payload
    Payload = #{<<"action">> => <<"add">>, <<"args">> => [15, 27]},
    Result = sofia_gateway:handle_request(mock_gateway_service, Payload, test_gw_breaker),
    ?assertEqual({ok, 42}, Result),
    
    ok = sofia_registry:deregister_service(mock_gateway_service, MockServer).

test_router() ->
    %% Start two servers with different responses
    Server1 = spawn(fun() ->
        receive
            {'$gen_call', From, {ping, _}} -> gen_server:reply(From, server1_ok)
        end
    end),
    Server2 = spawn(fun() ->
        receive
            {'$gen_call', From, {ping, _}} -> gen_server:reply(From, server2_ok)
        end
    end),
    
    ok = sofia_registry:register_service(mock_routed, Server1),
    ok = sofia_registry:register_service(mock_routed, Server2),
    
    %% Route dynamic request: if value is even route to Server1, if odd to Server2
    RouteFun = fun(Value, _Pids) ->
        case Value rem 2 of
            0 -> {ok, Server1};
            1 -> {ok, Server2}
        end
    end,
    
    {ok, TargetPid1} = sofia_router:route(mock_routed, 10, RouteFun),
    ?assertEqual(Server1, TargetPid1),
    
    {ok, TargetPid2} = sofia_router:route(mock_routed, 11, RouteFun),
    ?assertEqual(Server2, TargetPid2),
    
    ok = sofia_registry:deregister_service(mock_routed, Server1),
    ok = sofia_registry:deregister_service(mock_routed, Server2).

test_transformer() ->
    %% Test map-based key renaming
    Payload = #{src_key => "hello", keep_me => 123},
    Rules = #{src_key => dest_key},
    Result = sofia_transformer:transform(Payload, Rules),
    ?assertEqual(#{dest_key => "hello", keep_me => 123}, Result),
    
    %% Test function-based payload translation
    Fun = fun(P) -> P#{dest_key => maps:get(src_key, P) ++ " world"} end,
    Result2 = sofia_transformer:transform(Payload, Fun),
    ?assertEqual(#{src_key => "hello", keep_me => 123, dest_key => "hello world"}, Result2).

test_saga() ->
    %% Setup temporary ETS table to track state changes
    T = ets:new(saga_test_table, [public, set]),
    ets:insert(T, {step1, pending}),
    ets:insert(T, {step2, pending}),
    
    %% Saga Success Flow
    Step1Action = fun() -> ets:insert(T, {step1, done}), {ok, step1_result} end,
    Step1Compensate = fun(_) -> ets:insert(T, {step1, compensated}), ok end,
    
    Step2Action = fun() -> ets:insert(T, {step2, done}), {ok, step2_result} end,
    Step2Compensate = fun(_) -> ets:insert(T, {step2, compensated}), ok end,
    
    Steps = [
        {Step1Action, Step1Compensate},
        {Step2Action, Step2Compensate}
    ],
    
    ?assertEqual({ok, [step1_result, step2_result]}, sofia_saga:execute(Steps)),
    ?assertEqual([{step1, done}], ets:lookup(T, step1)),
    ?assertEqual([{step2, done}], ets:lookup(T, step2)),
    
    %% Reset state
    ets:insert(T, {step1, pending}),
    ets:insert(T, {step2, pending}),
    
    %% Saga Failure Flow (Step 2 fails, Step 1 compensations should be executed)
    Step2FailureAction = fun() -> {error, step2_failed} end,
    FailedSteps = [
        {Step1Action, Step1Compensate},
        {Step2FailureAction, Step2Compensate}
    ],
    
    Result = sofia_saga:execute(FailedSteps),
    ?assertEqual({error, {step_failed, step2_failed, [{ok, ok}]}}, Result),
    
    %% Verify step1 was executed then compensated, and step2 was never finalized
    ?assertEqual([{step1, compensated}], ets:lookup(T, step1)),
    
    ets:delete(T).

test_skeleton_and_stub() ->
    ServiceType = skeleton_test_service,
    
    %% Start the skeleton service
    {ok, ServicePid} = sofia_service_skeleton:start_link(ServiceType),
    ?assert(is_process_alive(ServicePid)),
    
    %% Verify discovery works (service registered during init)
    ?assertEqual({ok, ServicePid}, sofia_registry:discover(ServiceType)),
    
    %% Verify ping works via the service directly
    ?assertEqual(pong, sofia_service_skeleton:ping(ServicePid)),
    
    %% Verify ping works via the client stub
    ?assertEqual(pong, sofia_client_stub:ping_service(ServiceType)),
    
    %% Verify custom call works via the client stub
    ?assertEqual({ok, processed_payload}, sofia_client_stub:call_service(ServiceType, {custom_request, my_payload})),
    
    %% Stop the service
    ok = sofia_service_skeleton:stop(ServicePid),
    
    %% Verify the service is no longer alive
    ?assertEqual(false, is_process_alive(ServicePid)),
    
    %% Verify deregistration works (service deregistered during terminate)
    ?assertEqual({error, no_service_available}, sofia_registry:discover(ServiceType)).

test_healthcare_finance_interconnection() ->
    %% Start mock payment processor process running a loop
    Loop = fun L() ->
        receive
            {'$gen_call', From, {payment_gateway_charge, <<"pat_9901">>, 12500, <<"stripe">>}} ->
                gen_server:reply(From, {ok, stripe_tx_88921}),
                L();
            stop -> ok
        end
    end,
    ProcessorPid = spawn(Loop),
    ok = sofia_registry:register_service(payment_processor, ProcessorPid),

    %% 1. Gateway Request Ingestion
    BillingPayload = #{
        <<"action">> => <<"payment_gateway_charge">>,
        <<"patient_id">> => <<"pat_9901">>,
        <<"billing_cents">> => 12500,
        <<"method">> => <<"stripe">>
    },
    
    %% Gateway translates the request and routes it to payment_processor MockServer, returning the payment ref
    {ok, ChargeRef} = sofia_gateway:handle_request(payment_processor, BillingPayload, gateway_breaker),
    ?assertEqual(stripe_tx_88921, ChargeRef),

    %% 2. Transformer mapping patient keys to gateway format
    StripeSchemaRules = #{
        billing_cents => amount,
        patient_id => customer_ref
    },
    PaymentParams = sofia_transformer:transform(#{patient_id => <<"pat_9901">>, billing_cents => 12500}, StripeSchemaRules),
    ?assertEqual(#{customer_ref => <<"pat_9901">>, amount => 12500}, PaymentParams),

    %% 3. Router selects correct payment processor PID (mocked Stripe PID)
    ProcessorRouter = fun(Payload, Pids) ->
        case maps:get(method, Payload, <<"stripe">>) of
            <<"stripe">> -> {ok, hd(Pids)};
            <<"paypal">> -> {ok, lists:last(Pids)}
        end
    end,
    {ok, TargetProcessorPid} = sofia_router:route(payment_processor, #{method => <<"stripe">>}, ProcessorRouter),
    ?assertEqual(ProcessorPid, TargetProcessorPid),

    %% 4. Execute the call protected by a local circuit breaker
    CallFun = fun() -> gen_server:call(TargetProcessorPid, {payment_gateway_charge, <<"pat_9901">>, 12500, <<"stripe">>}) end,
    {ok, TxId} = sofia_breaker:call(stripe_breaker, CallFun),
    ?assertEqual(stripe_tx_88921, TxId),

    %% 5. Saga orchestration coordinates payment capture and appointment booking
    ChargePayment = fun() -> {ok, stripe_tx_88921} end,
    VoidPayment = fun(RefId) -> ?assertEqual(stripe_tx_88921, RefId), {ok, voided} end,

    %% Appointment booking fails to trigger compensation
    BookAppointmentFail = fun() -> {error, room_capacity_reached} end,
    CancelAppointment = fun(_) -> ok end,

    TransactionSteps = [
        {ChargePayment, VoidPayment},
        {BookAppointmentFail, CancelAppointment}
    ],

    SagaResult = sofia_saga:execute(TransactionSteps),
    ?assertEqual({error, {step_failed, room_capacity_reached, [{ok, {ok, voided}}]}}, SagaResult),

    ProcessorPid ! stop,
    ok = sofia_registry:deregister_service(payment_processor, ProcessorPid).

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
