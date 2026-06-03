-module(sofia_integration_tests).
-include_lib("eunit/include/eunit.hrl").

sofia_integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_gateway/0,
      fun test_router/0,
      fun test_transformer/0,
      fun test_saga/0,
      fun test_skeleton_and_stub/0,
      fun test_healthcare_finance_interconnection/0,
      fun test_http_gateway/0
     ]}.

setup() ->
    ok = application:start(sasl),
    {ok, Apps} = application:ensure_all_started(sofia),
    Apps.

cleanup(Apps) ->
    [application:stop(App) || App <- lists:reverse(Apps)],
    application:stop(sasl),
    ok.

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

test_http_gateway() ->
    ok = application:ensure_started(inets),
    
    Contract = #{
        methods => #{
            add => #{
                input_schema => #{
                    a => integer,
                    b => integer
                }
            }
        }
    },
    
    MockPid = spawn(fun L() ->
        receive
            {'$gen_call', From, {add, Payload}} ->
                A = maps:get(a, Payload),
                B = maps:get(b, Payload),
                gen_server:reply(From, {ok, A + B}),
                L();
            stop -> ok
        end
    end),
    
    ok = sofia_registry:register_service(http_calc_service, MockPid, Contract),
    
    timer:sleep(50),
    
    %% 1. Make a valid POST request to the Cowboy Edge Gateway
    ValidBody = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 10, <<"b">> => 20}}),
    {ok, {{_Version, 200, _}, _Headers, ResponseBody}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/http_calc_service",
                             [{"content-type", "application/json"}],
                             "application/json", ValidBody}, [], []),
                             
    Decoded = jsx:decode(list_to_binary(ResponseBody), [return_maps]),
    ?assertEqual(#{<<"status">> => <<"success">>, <<"result">> => 30}, Decoded),
    
    %% 2. Make an invalid POST request violating schema parameters (missing 'b')
    InvalidBody1 = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 10}}),
    {ok, {{_, 400, _}, _, ResponseBody2}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/http_calc_service",
                             [{"content-type", "application/json"}],
                             "application/json", InvalidBody1}, [], []),
                             
    Decoded2 = jsx:decode(list_to_binary(ResponseBody2), [return_maps]),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Decoded2)),
    ?assertEqual(<<"contract_validation_failed">>, maps:get(<<"reason">>, Decoded2)),
    
    %% 3. Make an invalid POST request violating schema types ('b' is not an integer)
    InvalidBody2 = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 10, <<"b">> => <<"not_int">>}}),
    {ok, {{_, 400, _}, _, ResponseBody3}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/http_calc_service",
                             [{"content-type", "application/json"}],
                             "application/json", InvalidBody2}, [], []),
                             
    Decoded3 = jsx:decode(list_to_binary(ResponseBody3), [return_maps]),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Decoded3)),
    ?assertEqual(<<"contract_validation_failed">>, maps:get(<<"reason">>, Decoded3)),
    
    %% Clean up
    MockPid ! stop,
    ok = sofia_registry:deregister_service(http_calc_service, MockPid),
    ok = application:stop(inets).
