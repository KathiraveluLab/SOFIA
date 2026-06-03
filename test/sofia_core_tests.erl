-module(sofia_core_tests).
-include_lib("eunit/include/eunit.hrl").

sofia_core_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_registry/0,
      fun test_breaker/0,
      fun test_config/0
     ]}.

setup() ->
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
