-module(benchmark_usecases).
-export([run/0]).

run() ->
    application:ensure_all_started(sofia),
    catch sofia_tracer:clear(),
    io:format("=== Profiling 5 Use Case Request Pipelines ===~n"),

    %% 1. Water Quality
    PidWater = water_quality:start_service(),
    ok = sofia_registry:register_service(water_service, PidWater),
    ok = sofia_config:set(water_ph_min, 650),
    ok = sofia_config:set(water_ph_max, 850),
    PayloadWater = #{
        <<"action">> => <<"add_potability_record">>,
        <<"sensor_id">> => <<"sensor_zone_A">>,
        <<"ph">> => 720,
        <<"ppm">> => 150
    },
    sofia_breaker:reset(water_breaker),
    LatWater = measure_loop(10000, fun() ->
        {ok, _} = sofia_gateway:handle_request(water_service, PayloadWater, water_breaker)
    end),
    ok = sofia_registry:deregister_service(water_service, PidWater),
    PidWater ! stop,

    %% 2. Carbon Traceability
    PidCarbonStd = carbon_traceability:start_auditor(standard),
    PidCarbonHigh = carbon_traceability:start_auditor(high_capacity),
    ok = sofia_registry:register_service(carbon_auditor, PidCarbonStd),
    ok = sofia_registry:register_service(carbon_auditor, PidCarbonHigh),
    AuditorRouter = fun(Payload, _Pids) ->
        Carbon = maps:get(co2_tons, Payload, 0),
        case Carbon > 500 of
            true -> {ok, PidCarbonHigh};
            false -> {ok, PidCarbonStd}
        end
    end,
    SchemaRules = #{temp_fahrenheit => temperature_f, carbon_tons => co2_tons},
    TelemetryData = #{mill_id => <<"steel_mill_09">>, carbon_tons => 620, temp_fahrenheit => 2200},
    LatCarbon = measure_loop(10000, fun() ->
        Norm = sofia_transformer:transform(TelemetryData, SchemaRules),
        {ok, Target} = sofia_router:route(carbon_auditor, Norm, AuditorRouter),
        gen_server:call(Target, {carbon_emissions_audit, maps:get(mill_id, TelemetryData), maps:get(co2_tons, Norm)})
    end),
    ok = sofia_registry:deregister_service(carbon_auditor, PidCarbonStd),
    ok = sofia_registry:deregister_service(carbon_auditor, PidCarbonHigh),
    PidCarbonStd ! stop,
    PidCarbonHigh ! stop,

    %% 3. Healthcare Outcome (Saga)
    SuccessSteps = [
        {fun() -> {ok, bed_id_104} end, fun(_B) -> ok end},
        {fun() -> {ok, insurance_ref_551} end, fun(_I) -> ok end}
    ],
    LatHealthcare = measure_loop(2000, fun() ->
        {ok, _} = sofia_saga:execute(SuccessSteps)
    end),

    %% 4. Education Credentials
    PidEdu1 = education_credentials:start_verifier(<<"mit.edu">>),
    PidEdu2 = education_credentials:start_verifier(<<"harvard.edu">>),
    ok = sofia_registry:register_service(credential_verifier, PidEdu1, #{}, #{<<"region">> => <<"us-east">>}),
    ok = sofia_registry:register_service(credential_verifier, PidEdu2, #{}, #{<<"region">> => <<"us-west">>}),
    LatEdu = measure_loop(5000, fun() ->
        {ok, TargetPid} = sofia_registry:discover_by_metadata(credential_verifier, #{<<"region">> => <<"us-east">>}),
        {ok, _} = gen_server:call(TargetPid, {verify_credential, <<"stu_123">>, <<"hash_456">>})
    end),
    ok = sofia_registry:deregister_service(credential_verifier, PidEdu1),
    ok = sofia_registry:deregister_service(credential_verifier, PidEdu2),
    PidEdu1 ! stop,
    PidEdu2 ! stop,

    %% 5. Portal Interconnection (Gateway + Transformer + Router + Breaker + Saga)
    ProcessorPid = spawn(fun L() ->
        receive
            {'$gen_call', From, {payment_gateway_charge, _, _, _}} ->
                gen_server:reply(From, {ok, stripe_tx_88921}),
                L();
            stop -> ok;
            _ -> L()
        end
    end),
    ok = sofia_registry:register_service(payment_processor, ProcessorPid),
    BillingPayload = #{
        <<"action">> => <<"payment_gateway_charge">>,
        <<"patient_id">> => <<"pat_9901">>,
        <<"billing_cents">> => 12500,
        <<"method">> => <<"stripe">>
    },
    StripeRules = #{billing_cents => amount, patient_id => customer_ref},
    ProcRouter = fun(_P, Pids) -> {ok, hd(Pids)} end,
    ChargePayment = fun() -> {ok, stripe_tx_88921} end,
    VoidPayment = fun(_) -> {ok, voided} end,
    BookAppointment = fun() -> {ok, appt_booked} end,
    CancelAppointment = fun(_) -> ok end,
    PortalSagaSteps = [
        {ChargePayment, VoidPayment},
        {BookAppointment, CancelAppointment}
    ],
    sofia_breaker:reset(gateway_breaker),
    sofia_breaker:reset(stripe_breaker),
    LatPortal = measure_loop(2000, fun() ->
        {ok, _} = sofia_gateway:handle_request(payment_processor, BillingPayload, gateway_breaker),
        _NormParams = sofia_transformer:transform(#{patient_id => <<"pat_9901">>, billing_cents => 12500}, StripeRules),
        {ok, TargetProc} = sofia_router:route(payment_processor, #{method => <<"stripe">>}, ProcRouter),
        {ok, _} = sofia_breaker:call(stripe_breaker, fun() ->
            gen_server:call(TargetProc, {payment_gateway_charge, <<"pat_9901">>, 12500, <<"stripe">>})
        end),
        {ok, _} = sofia_saga:execute(PortalSagaSteps)
    end),
    ok = sofia_registry:deregister_service(payment_processor, ProcessorPid),
    ProcessorPid ! stop,

    print_results("Municipal Water Quality", LatWater, 500.0),
    print_results("Industrial Carbon Traceability", LatCarbon, 750.0),
    print_results("Healthcare Outcome Verification", LatHealthcare, 1200.0),
    print_results("Education Credentials Verification", LatEdu, 400.0),
    print_results("Healthcare & Finance Interconnection", LatPortal, 1500.0),
    ok.

measure_loop(Iterations, Fun) ->
    [begin
        T1 = erlang:system_time(nanosecond),
        Fun(),
        T2 = erlang:system_time(nanosecond),
        (T2 - T1) / 1000.0 %% microsecond
    end || _ <- lists:seq(1, Iterations)].

print_results(Name, Latencies, SLATarget) ->
    Mean = lists:sum(Latencies) / length(Latencies),
    Sorted = lists:sort(Latencies),
    P99Index = max(1, round(length(Sorted) * 0.99)),
    P99 = lists:nth(P99Index, Sorted),
    Min = hd(Sorted),
    Max = lists:last(Sorted),
    Violations = length([L || L <- Latencies, L > SLATarget]),
    SLAComp = 100.0 * (1.0 - Violations / length(Latencies)),
    io:format("~s:~n  Mean: ~.2f us | Min: ~.2f us | P99: ~.2f us | Max: ~.2f us | SLA (<~b us): ~.2f% (Violations: ~b/~b)~n",
              [Name, Mean, Min, P99, Max, round(SLATarget), SLAComp, Violations, length(Latencies)]).
