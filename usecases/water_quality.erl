-module(water_quality).
-export([run/0, start_service/0, service_loop/0]).

start_service() ->
    spawn(fun service_loop/0).

service_loop() ->
    receive
        {'$gen_call', From, {add_potability_record, SensorId, PH, PPM}} ->
            PHMin = sofia_config:get(water_ph_min, 650),
            PHMax = sofia_config:get(water_ph_max, 850),
            Status = case (PH >= PHMin) and (PH =< PHMax) of
                true -> safe;
                false -> unsafe
            end,
            gen_server:reply(From, {ok, #{sensor => SensorId, status => Status, ph => PH, ppm => PPM}});
        {'$gen_call', From, simulate_timeout} ->
            gen_server:reply(From, {error, timeout});
        stop ->
            ok;
        _ ->
            service_loop()
    end,
    service_loop().

run() ->
    Pid = start_service(),
    ok = sofia_registry:register_service(water_service, Pid),

    %% 1. Set global safety thresholds in sofia_config
    ok = sofia_config:set(water_ph_min, 650),
    ok = sofia_config:set(water_ph_max, 850),

    %% 2. Ingest JSON-like map through gateway
    PayloadSafe = #{
        <<"action">> => <<"add_potability_record">>,
        <<"sensor_id">> => <<"sensor_zone_A">>,
        <<"ph">> => 720,
        <<"ppm">> => 150
    },
    {ok, ResultSafe} = sofia_gateway:handle_request(water_service, PayloadSafe, water_breaker),
    #{status := safe} = ResultSafe,

    PayloadUnsafe = #{
        <<"action">> => <<"add_potability_record">>,
        <<"sensor_id">> => <<"sensor_zone_B">>,
        <<"ph">> => 900,
        <<"ppm">> => 210
    },
    {ok, ResultUnsafe} = sofia_gateway:handle_request(water_service, PayloadUnsafe, water_breaker),
    #{status := unsafe} = ResultUnsafe,

    %% 3. Circuit Breaker Simulation
    ok = sofia_breaker:reset(water_breaker),
    {ok, closed} = sofia_breaker:get_state(water_breaker),

    %% Trigger failures to trip the circuit breaker
    FailingFun = fun() -> gen_server:call(Pid, simulate_timeout) end,
    {error, timeout} = sofia_breaker:call(water_breaker, FailingFun, #{max_failures => 3}),
    {error, timeout} = sofia_breaker:call(water_breaker, FailingFun, #{max_failures => 3}),
    {error, timeout} = sofia_breaker:call(water_breaker, FailingFun, #{max_failures => 3}),

    %% The breaker should now be open
    {ok, open} = sofia_breaker:get_state(water_breaker),
    {error, circuit_open} = sofia_breaker:call(water_breaker, FailingFun, #{max_failures => 3}),

    %% Clean up
    ok = sofia_registry:deregister_service(water_service, Pid),
    Pid ! stop,
    ok.
