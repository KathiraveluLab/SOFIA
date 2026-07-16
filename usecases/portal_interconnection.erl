-module(portal_interconnection).
-export([run/0]).

run() ->
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
    
    %% Gateway translates the request and routes it to payment_processor, returning the payment ref
    {ok, ChargeRef} = sofia_gateway:handle_request(payment_processor, BillingPayload, gateway_breaker),
    stripe_tx_88921 = ChargeRef,

    %% 2. Transformer mapping patient keys to gateway format
    StripeSchemaRules = #{
        billing_cents => amount,
        patient_id => customer_ref
    },
    PaymentParams = sofia_transformer:transform(#{patient_id => <<"pat_9901">>, billing_cents => 12500}, StripeSchemaRules),
    #{customer_ref := <<"pat_9901">>, amount := 12500} = PaymentParams,

    %% 3. Router selects correct payment processor PID (mocked Stripe PID)
    ProcessorRouter = fun(Payload, Pids) ->
        case maps:get(method, Payload, <<"stripe">>) of
            <<"stripe">> -> {ok, hd(Pids)};
            <<"paypal">> -> {ok, lists:last(Pids)}
        end
    end,
    {ok, TargetProcessorPid} = sofia_router:route(payment_processor, #{method => <<"stripe">>}, ProcessorRouter),
    ProcessorPid = TargetProcessorPid,

    %% 4. Execute the call protected by a local circuit breaker
    CallFun = fun() -> gen_server:call(TargetProcessorPid, {payment_gateway_charge, <<"pat_9901">>, 12500, <<"stripe">>}) end,
    {ok, TxId} = sofia_breaker:call(stripe_breaker, CallFun),
    stripe_tx_88921 = TxId,

    %% 5. Saga orchestration coordinates payment capture and appointment booking
    ChargePayment = fun() -> {ok, stripe_tx_88921} end,
    VoidPayment = fun(RefId) -> stripe_tx_88921 = RefId, {ok, voided} end,

    %% Appointment booking fails to trigger compensation
    BookAppointmentFail = fun() -> {error, room_capacity_reached} end,
    CancelAppointment = fun(_) -> ok end,

    TransactionSteps = [
        {ChargePayment, VoidPayment},
        {BookAppointmentFail, CancelAppointment}
    ],

    SagaResult = sofia_saga:execute(TransactionSteps),
    {error, {step_failed, room_capacity_reached, [{ok, {ok, voided}}]}} = SagaResult,

    ProcessorPid ! stop,
    ok = sofia_registry:deregister_service(payment_processor, ProcessorPid).
