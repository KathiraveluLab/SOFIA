-module(carbon_traceability).
-export([run/0, start_auditor/1, auditor_loop/1]).

start_auditor(Type) ->
    spawn(fun() -> auditor_loop(Type) end).

auditor_loop(Type) ->
    receive
        {'$gen_call', From, {carbon_emissions_audit, MillId, Tons}} ->
            gen_server:reply(From, {ok, {Type, MillId, Tons}});
        stop ->
            ok;
        _ ->
            auditor_loop(Type)
    end,
    auditor_loop(Type).

run() ->
    StandardPid = start_auditor(standard),
    HighPid = start_auditor(high_capacity),

    ok = sofia_registry:register_service(carbon_auditor, StandardPid),
    ok = sofia_registry:register_service(carbon_auditor, HighPid),

    %% 1. Dynamic content-based routing
    AuditorRouter = fun(Payload, _Pids) ->
        Carbon = maps:get(carbon_tons, Payload),
        case Carbon > 500 of
            true -> {ok, HighPid}; % Route to specialized high-capacity auditor
            false -> {ok, StandardPid} % Route to standard auditor
        end
    end,

    %% Route Standard Payload
    PayloadStandard = #{mill_id => <<"steel_mill_04">>, carbon_tons => 350},
    {ok, TargetPid1} = sofia_router:route(carbon_auditor, PayloadStandard, AuditorRouter),
    StandardPid = TargetPid1,

    %% Route High Payload
    PayloadHigh = #{mill_id => <<"steel_mill_09">>, carbon_tons => 620},
    {ok, TargetPid2} = sofia_router:route(carbon_auditor, PayloadHigh, AuditorRouter),
    HighPid = TargetPid2,

    %% 2. Data transformation and mapping using sofia_transformer
    TelemetryData = #{
        mill_id => <<"steel_mill_09">>,
        carbon_tons => 620,
        temp_fahrenheit => 2200
    },
    SchemaRules = #{
        temp_fahrenheit => temperature_f,
        carbon_tons => co2_tons
    },
    NormalizedData = sofia_transformer:transform(TelemetryData, SchemaRules),
    #{co2_tons := 620, temperature_f := 2200} = NormalizedData,

    %% Clean up
    ok = sofia_registry:deregister_service(carbon_auditor, StandardPid),
    ok = sofia_registry:deregister_service(carbon_auditor, HighPid),
    StandardPid ! stop,
    HighPid ! stop,
    ok.
