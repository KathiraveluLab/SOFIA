-module(sofia_integration_tests).
-include_lib("eunit/include/eunit.hrl").
-export([recovery_compensate/2]).

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
      fun test_http_gateway/0,
      fun test_http_gateway_auth/0,
      fun test_http_gateway_openapi/0,
      fun test_backpressure/0,
      fun test_saga_recovery/0,
      fun test_qos_routing/0,
      fun test_rate_limiter/0,
      fun test_dlq/0
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

test_backpressure() ->
    %% Configure mailbox limit to 5 for testing purposes
    application:set_env(sofia, max_mailbox_size, 5),
    
    %% Start a mock service that only processes '$gen_call' pings
    IdlePid = spawn(fun Loop() ->
        receive
            {'$gen_call', From, ping} ->
                gen_server:reply(From, pong),
                Loop()
        end
    end),
    ok = sofia_registry:register_service(backpressured_service, IdlePid),
    
    ?assertEqual(pong, sofia_client_stub:call_service(backpressured_service, ping)),
    
    %% Send 10 messages directly to the process's mailbox to exceed limit (5)
    [IdlePid ! {dummy, N} || N <- lists:seq(1, 10)],
    
    %% call_service now routes through sofia_router which returns {error, overloaded}
    %% when all Pids are filtered out due to mailbox overflow
    ?assertMatch({error, _}, sofia_client_stub:call_service(backpressured_service, ping)),
    
    ?assertEqual({error, overloaded}, sofia_gateway:handle_request(backpressured_service, #{<<"action">> => <<"add">>, <<"args">> => [1, 2]}, test_breaker)),
    
    %% Route dynamic request: router should return {error, overloaded} since the only registered Pid is overloaded
    RouteFun = fun(_Payload, Pids) -> {ok, hd(Pids)} end,
    ?assertEqual({error, overloaded}, sofia_router:route(backpressured_service, dummy_payload, RouteFun)),
    
    %% Clean up
    ok = sofia_registry:deregister_service(backpressured_service, IdlePid),
    exit(IdlePid, kill),
    
    %% Reset mailbox limit to default (100)
    application:set_env(sofia, max_mailbox_size, 100).

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

test_http_gateway_auth() ->
    ok = application:ensure_started(inets),
    
    %% Set client secret in the auth database
    ok = sofia_auth:set_client_secret(<<"client_123">>, <<"super_secret_key">>),
    
    Contract = #{
        security => hmac,
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
    
    ok = sofia_registry:register_service(secure_calc_service, MockPid, Contract),
    
    timer:sleep(50),
    
    ValidBody = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 10, <<"b">> => 20}}),
    
    %% 1. Make an unauthenticated request (expecting 401 Unauthorized)
    {ok, {{_Version, 401, _}, _Headers1, ResponseBody1}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/secure_calc_service",
                             [{"content-type", "application/json"}],
                             "application/json", ValidBody}, [], []),
                             
    Decoded1 = jsx:decode(list_to_binary(ResponseBody1), [return_maps]),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Decoded1)),
    ?assertEqual(<<"missing_auth_headers">>, maps:get(<<"reason">>, Decoded1)),
    
    %% 2. Make an request with invalid signature (expecting 403 Forbidden)
    CurrentTimestamp = integer_to_binary(erlang:system_time(second)),
    {ok, {{_, 403, _}, _, ResponseBody2}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/secure_calc_service",
                             [{"content-type", "application/json"},
                              {"x-sofia-client-id", "client_123"},
                              {"x-sofia-signature", "invalid_sig_here"},
                              {"x-sofia-timestamp", binary_to_list(CurrentTimestamp)}],
                             "application/json", ValidBody}, [], []),
                             
    Decoded2 = jsx:decode(list_to_binary(ResponseBody2), [return_maps]),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Decoded2)),
    ?assertEqual(<<"forbidden">>, maps:get(<<"reason">>, Decoded2)),
    ?assertEqual(<<"invalid_signature">>, maps:get(<<"details">>, Decoded2)),
    
    %% 3. Make a valid signed request (expecting 200 OK)
    Timestamp = integer_to_binary(erlang:system_time(second)),
    {ok, SignatureHex} = sofia_auth:sign_payload(<<"client_123">>, Timestamp, ValidBody),
    
    {ok, {{_, 200, _}, _, ResponseBody3}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/secure_calc_service",
                             [{"content-type", "application/json"},
                              {"x-sofia-client-id", "client_123"},
                              {"x-sofia-signature", binary_to_list(SignatureHex)},
                              {"x-sofia-timestamp", binary_to_list(Timestamp)}],
                             "application/json", ValidBody}, [], []),
                             
    Decoded3 = jsx:decode(list_to_binary(ResponseBody3), [return_maps]),
    ?assertEqual(#{<<"status">> => <<"success">>, <<"result">> => 30}, Decoded3),
    
    %% Clean up
    MockPid ! stop,
    ok = sofia_registry:deregister_service(secure_calc_service, MockPid),
    ok = application:stop(inets).

test_http_gateway_openapi() ->
    ok = application:ensure_started(inets),
    Contract = #{
        version => <<"1.2.3">>,
        methods => #{
            add => #{
                input_schema => #{
                    a => integer,
                    b => integer
                }
            }
        }
    },
    MockPid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ok = sofia_registry:register_service(openapi_test_service, MockPid, Contract),
    timer:sleep(50),
    
    %% Make a GET request to obtain the dynamic OpenAPI spec
    {ok, {{_Version, 200, _}, _Headers, ResponseBody}} = 
        httpc:request(get, {"http://localhost:8080/api/v1/service/openapi_test_service", []}, [], []),
    
    Decoded = jsx:decode(list_to_binary(ResponseBody), [return_maps]),
    ?assertEqual(<<"3.0.0">>, maps:get(<<"openapi">>, Decoded)),
    Info = maps:get(<<"info">>, Decoded),
    ?assertEqual(<<"openapi_test_service">>, maps:get(<<"title">>, Info)),
    ?assertEqual(<<"1.2.3">>, maps:get(<<"version">>, Info)),
    
    Paths = maps:get(<<"paths">>, Decoded),
    ?assert(maps:is_key(<<"/api/v1/service/openapi_test_service">>, Paths)),
    
    %% Test 404 response for unregistered service
    {ok, {{_, 404, _}, _, ResponseBody2}} = 
        httpc:request(get, {"http://localhost:8080/api/v1/service/non_existent_contract_service", []}, [], []),
    Decoded2 = jsx:decode(list_to_binary(ResponseBody2), [return_maps]),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Decoded2)),
    ?assertEqual(<<"no_contract_registered">>, maps:get(<<"reason">>, Decoded2)),
    
    MockPid ! stop,
    ok = sofia_registry:deregister_service(openapi_test_service, MockPid),
    ok = application:stop(inets).

recovery_compensate(Table, Result) ->
    ets:insert(Table, {compensated, Result}),
    ok.

test_saga_recovery() ->
    Table = ets:new(recovery_test_table, [public, set]),
    
    %% Manually insert a crashed "running" saga into the Mnesia table
    SagaId = make_ref(),
    
    %% Define serializable MFA-based steps
    %% Step 1 has a valid MFA compensation: {sofia_integration_tests, recovery_compensate, [Table]}
    %% Step 2 has not started yet
    Steps = [
        {{erlang, self, []}, {sofia_integration_tests, recovery_compensate, [Table]}},
        {{erlang, self, []}, {sofia_integration_tests, recovery_compensate, [Table]}}
    ],
    
    Record = {sofia_sagas, SagaId, running, [{1, step1_result}], 2, Steps},
    F = fun() -> mnesia:write(Record) end,
    {atomic, ok} = mnesia:transaction(F),
    
    %% Trigger WAL recovery manually
    ok = sofia_saga:recover_sagas(),
    
    %% Assert that the compensation for Step 1 was executed via recovery
    ?assertEqual([{compensated, step1_result}], ets:lookup(Table, compensated)),
    
    %% Assert that the saga state has been updated to rolled_back
    FRead = fun() -> mnesia:read(sofia_sagas, SagaId) end,
    {atomic, [UpdatedRecord]} = mnesia:transaction(FRead),
    ?assertEqual(rolled_back, element(3, UpdatedRecord)),
    
    ets:delete(Table).

test_qos_routing() ->
    QosServer1 = spawn(fun() -> receive hold -> ok end end),
    QosServer2 = spawn(fun() -> receive hold -> ok end end),
    
    ok = sofia_registry:register_service(qos_service, QosServer1),
    ok = sofia_registry:register_service(qos_service, QosServer2),
    
    %% Send 3 unmatched messages to QosServer1 to increase its mailbox queue length
    QosServer1 ! dummy1,
    QosServer1 ! dummy2,
    QosServer1 ! dummy3,
    
    %% Route: RouteFun simply returns the first Pid in the list
    RouteFun = fun(_, SortedPids) -> {ok, hd(SortedPids)} end,
    
    %% The router should rank QosServer2 first because it has a shorter mailbox queue
    ?assertEqual({ok, QosServer2}, sofia_router:route(qos_service, test_payload, RouteFun)),
    
    %% Simulate an open circuit breaker on our local node for qos_service
    %% sofia_breakers_table is named_table
    ets:insert(sofia_breakers_table, {qos_service, open, 3, erlang:system_time(millisecond)}),
    ?assertEqual({error, overloaded}, sofia_router:route(qos_service, test_payload, RouteFun)),
    
    %% Clean up
    ets:delete(sofia_breakers_table, qos_service),
    ok = sofia_registry:deregister_service(qos_service, QosServer1),
    ok = sofia_registry:deregister_service(qos_service, QosServer2),
    exit(QosServer1, kill),
    exit(QosServer2, kill).

test_rate_limiter() ->
    ok = application:ensure_started(inets),
    
    %% Set strict SLA limit: 1.0 tokens/sec, max capacity 1.0
    ok = sofia_rate_limiter:set_sla(<<"test_client">>, 1.0, 1.0),
    
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
    ok = sofia_registry:register_service(rate_limited_service, MockPid, Contract),
    timer:sleep(50),
    
    ValidBody = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 10, <<"b">> => 20}}),
    
    %% First request should succeed (200 OK)
    {ok, {{_, 200, _}, _, ResponseBody1}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/rate_limited_service",
                             [{"content-type", "application/json"},
                              {"x-sofia-client-id", "test_client"}],
                             "application/json", ValidBody}, [], []),
    Decoded1 = jsx:decode(list_to_binary(ResponseBody1), [return_maps]),
    ?assertEqual(#{<<"status">> => <<"success">>, <<"result">> => 30}, Decoded1),
    
    %% Second request immediately following should be rate limited (429 Too Many Requests)
    {ok, {{_, 429, _}, _, ResponseBody2}} = 
        httpc:request(post, {"http://localhost:8080/api/v1/service/rate_limited_service",
                             [{"content-type", "application/json"},
                              {"x-sofia-client-id", "test_client"}],
                             "application/json", ValidBody}, [], []),
    Decoded2 = jsx:decode(list_to_binary(ResponseBody2), [return_maps]),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Decoded2)),
    ?assertEqual(<<"rate_limited">>, maps:get(<<"reason">>, Decoded2)),
    
    %% Clean up
    MockPid ! stop,
    ok = sofia_registry:deregister_service(rate_limited_service, MockPid),
    ok = application:stop(inets),
    
    %% Clear SLA record from Mnesia
    F = fun() -> mnesia:delete({sofia_slas, <<"test_client">>}) end,
    mnesia:transaction(F).

test_dlq() ->
    ok = application:ensure_started(inets),

    %% Step 1: Purge any pre-existing DLQ entries
    ok = sofia_dlq:purge(),
    {ok, []} = sofia_dlq:list(),

    %% Step 2: Register a service with a strict schema contract
    Contract = #{methods => #{add => #{input_schema => #{a => integer, b => integer}}}},
    MockPid = spawn(fun L() ->
        receive
            {'$gen_call', From, {add, Payload}} ->
                A = maps:get(a, Payload), B = maps:get(b, Payload),
                gen_server:reply(From, {ok, A + B}), L();
            stop -> ok
        end
    end),
    ok = sofia_registry:register_service(dlq_calc_service, MockPid, Contract),
    timer:sleep(50),

    %% Step 3: Send a request with a type-violating payload (b is not an integer)
    BadBody = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 5, <<"b">> => <<"not_int">>}}),
    {ok, {{_, 400, _}, _, _}} =
        httpc:request(post, {"http://localhost:8080/api/v1/service/dlq_calc_service",
                             [{"content-type", "application/json"}],
                             "application/json", BadBody}, [], []),

    %% Step 4: Send a request with a missing parameter (b is absent)
    MissingBody = jsx:encode(#{<<"method">> => <<"add">>, <<"payload">> => #{<<"a">> => 5}}),
    {ok, {{_, 400, _}, _, _}} =
        httpc:request(post, {"http://localhost:8080/api/v1/service/dlq_calc_service",
                             [{"content-type", "application/json"}],
                             "application/json", MissingBody}, [], []),

    %% Step 5: DLQ should now have exactly 2 entries for dlq_calc_service
    {ok, AllEntries} = sofia_dlq:list(dlq_calc_service),
    ?assertEqual(2, length(AllEntries)),

    %% Step 6: Verify each entry has the expected fields
    [Entry | _] = AllEntries,
    ?assert(maps:is_key(entry_id, Entry)),
    ?assert(maps:is_key(timestamp, Entry)),
    ?assertEqual(dlq_calc_service, maps:get(service, Entry)),

    %% Step 7: Purge and confirm empty
    ok = sofia_dlq:purge(),
    {ok, []} = sofia_dlq:list(dlq_calc_service),

    %% Cleanup
    MockPid ! stop,
    ok = sofia_registry:deregister_service(dlq_calc_service, MockPid),
    ok = application:stop(inets).

