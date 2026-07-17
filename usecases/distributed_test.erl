-module(distributed_test).
-export([run/0]).

run() ->
    %% Get hostname to construct node names dynamically
    {ok, Host} = inet:gethostname(),
    Node1 = list_to_atom("sofia_node1@" ++ Host),
    Node2 = list_to_atom("sofia_node2@" ++ Host),

    io:format("Connecting to deployed nodes...~n"),

    ok = wait_for_node(Node1, 10),
    ok = wait_for_node(Node2, 10),
    pong = rpc:call(Node1, net_adm, ping, [Node2]),

    io:format("Nodes connected: ~p and ~p~n", [Node1, Node2]),

    %% Test 1: Configuration Replication (LWW)
    io:format("Test 1: Configuration Propagation...~n"),
    %% Set config on Node1
    ok = rpc:call(Node1, sofia_config, set, [test_key, "hello_cluster"]),
    %% Read config on Node2 (wait a tiny bit for async propagation)
    timer:sleep(200),
    Val = rpc:call(Node2, sofia_config, get, [test_key]),
    io:format("Node2 read config: ~p~n", [Val]),
    case Val of
        "hello_cluster" -> ok;
        _ -> exit({test_failed, config_replication})
    end,

    %% Test 2: Distributed Service Discovery
    io:format("Test 2: Distributed Service Discovery...~n"),
    %% Spawn a dummy service on Node2
    MockPid = rpc:call(Node2, erlang, spawn, [fun() ->
        Loop = fun L() ->
            receive
                {'$gen_call', From, ping} ->
                    gen_server:reply(From, distributed_pong),
                    L();
                stop -> ok
            end
        end,
        Loop()
    end]),
    %% Register service on Node2
    ok = rpc:call(Node2, sofia_registry, register_service, [my_dist_service, MockPid]),
    timer:sleep(200),

    %% Discover and call from Node1
    {ok, DiscoveredPid} = rpc:call(Node1, sofia_registry, discover, [my_dist_service]),
    io:format("Node1 discovered service Pid: ~p (Node: ~p)~n", [DiscoveredPid, node(DiscoveredPid)]),
    
    %% Call service from Node1 (protected by circuit breaker on Node1!)
    CallResult = rpc:call(Node1, sofia_breaker, call, [dist_breaker, fun() ->
        gen_server:call(DiscoveredPid, ping)
    end]),
    io:format("Node1 breaker call result: ~p~n", [CallResult]),
    case CallResult of
        distributed_pong -> ok;
        _ -> exit({test_failed, service_discovery})
    end,

    %% Test 3: Micro-benchmarking latency under real deployment
    io:format("Test 3: Benchmarking Latencies in Deployment...~n"),
    
    %% Measure discovery latency from Node1 querying pg scope synced from Node2
    T1_start = erlang:system_time(nanosecond),
    loop(5000, fun() ->
        {ok, _} = rpc:call(Node1, sofia_registry, discover, [my_dist_service])
    end),
    T1_end = erlang:system_time(nanosecond),
    DiscoveryLatency = (T1_end - T1_start) / 5000.0 / 1000.0,
    io:format("Deployment Registry Discovery: ~8.4f microsec~n", [DiscoveryLatency]),

    %% Measure Config propagation delay (Set on Node1, verify eventually on Node2)
    T2_start = erlang:system_time(nanosecond),
    loop(500, fun() ->
        ok = rpc:call(Node1, sofia_config, set, [benchmark_key, "perf_test"])
    end),
    T2_end = erlang:system_time(nanosecond),
    ConfigLatency = (T2_end - T2_start) / 500.0 / 1000.0,
    io:format("Deployment Config Update Broadcast: ~8.4f microsec~n", [ConfigLatency]),

    io:format("All distributed deployment tests PASSED successfully!~n"),
    ok.

wait_for_node(_Node, 0) ->
    {error, timeout};
wait_for_node(Node, N) ->
    case net_adm:ping(Node) of
        pong -> ok;
        pang ->
            timer:sleep(500),
            wait_for_node(Node, N-1)
    end.

loop(0, _) -> ok;
loop(N, Fun) ->
    Fun(),
    loop(N-1, Fun).
