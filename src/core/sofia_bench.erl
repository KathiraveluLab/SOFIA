-module(sofia_bench).
-export([run/0]).

run() ->
    io:format("Starting SOFIA Benchmarks...~n"),
    %% Setup applications
    application:start(sasl),
    application:ensure_all_started(sofia),

    %% 1. Direct local call
    DirectFun = fun() -> ok end,
    T1_start = erlang:system_time(nanosecond),
    loop(100000, DirectFun),
    T1_end = erlang:system_time(nanosecond),
    DirectLatency = (T1_end - T1_start) / 100000.0 / 1000.0, %% microsec

    %% 2. Circuit breaker call (closed)
    BreakerFun = fun() -> ok end,
    sofia_breaker:call(bench_service, BreakerFun),
    T2_start = erlang:system_time(nanosecond),
    loop(100000, fun() -> sofia_breaker:call(bench_service, BreakerFun) end),
    T2_end = erlang:system_time(nanosecond),
    BreakerLatency = (T2_end - T2_start) / 100000.0 / 1000.0, %% microsec

    %% 3. Registry discover
    Pid = self(),
    sofia_registry:register_service(bench_service, Pid),
    T3_start = erlang:system_time(nanosecond),
    loop(50000, fun() -> sofia_registry:discover(bench_service) end),
    T3_end = erlang:system_time(nanosecond),
    RegistryLatency = (T3_end - T3_start) / 50000.0 / 1000.0, %% microsec
    sofia_registry:deregister_service(bench_service, Pid),

    %% 4. Rate Limiter check (Mnesia transaction)
    sofia_rate_limiter:set_sla(<<"bench_client">>, 1000000.0, 1000000.0), %% very high rate so we don't get limited
    T4_start = erlang:system_time(nanosecond),
    loop(20000, fun() -> sofia_rate_limiter:check_rate(<<"bench_client">>) end),
    T4_end = erlang:system_time(nanosecond),
    RateLimitLatency = (T4_end - T4_start) / 20000.0 / 1000.0, %% microsec

    %% 5. DLQ Enqueue (Mnesia transaction)
    T5_start = erlang:system_time(nanosecond),
    loop(20000, fun() -> sofia_dlq:enqueue(bench_service, test_reason, <<"payload">>, <<"client">>) end),
    T5_end = erlang:system_time(nanosecond),
    DLQLatency = (T5_end - T5_start) / 20000.0 / 1000.0, %% microsec

    %% 6. Config Set (ETS write + cast)
    T6_start = erlang:system_time(nanosecond),
    loop(50000, fun() -> sofia_config:set(bench_config_key, 42) end),
    T6_end = erlang:system_time(nanosecond),
    ConfigLatency = (T6_end - T6_start) / 50000.0 / 1000.0, %% microsec

    %% 7. TCP Loopback Roundtrip (Simulated SOTA Sidecar/Cache network overhead)
    {ok, TCPPort, ListenSocket, EchoServerPid} = start_echo_server(),
    {ok, Socket} = gen_tcp:connect("localhost", TCPPort, [binary, {packet, 0}, {active, false}]),
    T7_start = erlang:system_time(nanosecond),
    loop(5000, fun() ->
        gen_tcp:send(Socket, <<"ping">>),
        {ok, <<"ping">>} = gen_tcp:recv(Socket, 0)
    end),
    T7_end = erlang:system_time(nanosecond),
    TCPRoundtrip = (T7_end - T7_start) / 5000.0 / 1000.0,
    gen_tcp:close(Socket),
    gen_tcp:close(ListenSocket),
    exit(EchoServerPid, kill),

    %% 8. HTTP Gateway Roundtrip (Simulated Consul/Registry HTTP REST lookup)
    inets:start(),
    T8_start = erlang:system_time(nanosecond),
    loop(500, fun() ->
        {ok, {{_, 200, _}, _, _}} = httpc:request(get, {"http://localhost:8080/health", []}, [], [])
    end),
    T8_end = erlang:system_time(nanosecond),
    HTTPRoundtrip = (T8_end - T8_start) / 500.0 / 1000.0,

    %% 9. Real Redis GET Latency (over local TCP loopback)
    RedisLatency = case gen_tcp:connect("localhost", 6379, [binary, {active, false}]) of
        {ok, RedisSocket} ->
            %% Warm up & Set key
            ok = gen_tcp:send(RedisSocket, <<"*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$2\r\n42\r\n">>),
            {ok, _} = gen_tcp:recv(RedisSocket, 0),
            T9_start = erlang:system_time(nanosecond),
            loop(5000, fun() ->
                ok = gen_tcp:send(RedisSocket, <<"*2\r\n$3\r\nGET\r\n$8\r\ntest_key\r\n">>),
                {ok, _} = gen_tcp:recv(RedisSocket, 0)
            end),
            T9_end = erlang:system_time(nanosecond),
            gen_tcp:close(RedisSocket),
            (T9_end - T9_start) / 5000.0 / 1000.0;
        _ ->
            undefined
    end,

    %% 10. Real Consul KV GET Latency (over local HTTP REST)
    ConsulConfigLatency = case httpc:request(put, {"http://localhost:8500/v1/kv/test_key", [], "text/plain", "42"}, [], []) of
        {ok, _} ->
            T10_start = erlang:system_time(nanosecond),
            loop(500, fun() ->
                {ok, {{_, 200, _}, _, _}} = httpc:request(get, {"http://localhost:8500/v1/kv/test_key", []}, [], [])
            end),
            T10_end = erlang:system_time(nanosecond),
            (T10_end - T10_start) / 500.0 / 1000.0;
        _ ->
            undefined
    end,

    %% 11. Real Consul Service Discovery Latency (over local HTTP REST)
    ConsulDiscLatency = case httpc:request(put, {"http://localhost:8500/v1/agent/service/register", [], "application/json", "{\"ID\": \"test_service\", \"Name\": \"test_service\", \"Address\": \"127.0.0.1\", \"Port\": 8080}"}, [], []) of
        {ok, _} ->
            T11_start = erlang:system_time(nanosecond),
            loop(500, fun() ->
                {ok, {{_, 200, _}, _, _}} = httpc:request(get, {"http://localhost:8500/v1/catalog/service/test_service", []}, [], [])
            end),
            T11_end = erlang:system_time(nanosecond),
            httpc:request(put, {"http://localhost:8500/v1/agent/service/deregister/test_service", [], "", ""}, [], []),
            (T11_end - T11_start) / 500.0 / 1000.0;
        _ ->
            undefined
    end,

    io:format("Direct Local Call: ~8.4f microsec~n", [DirectLatency]),
    io:format("Circuit Breaker Call (Closed): ~8.4f microsec~n", [BreakerLatency]),
    io:format("Registry Service Discovery: ~8.4f microsec~n", [RegistryLatency]),
    io:format("Rate Limiter Check (Mnesia): ~8.4f microsec~n", [RateLimitLatency]),
    io:format("DLQ Enqueue (Mnesia): ~8.4f microsec~n", [DLQLatency]),
    io:format("Config Update (ETS + RPC): ~8.4f microsec~n", [ConfigLatency]),
    io:format("Simulated TCP Loopback Roundtrip (SOTA Network Baseline): ~8.4f microsec~n", [TCPRoundtrip]),
    io:format("Simulated HTTP Gateway Roundtrip (Consul/REST Baseline): ~8.4f microsec~n", [HTTPRoundtrip]),
    case RedisLatency of
        undefined -> ok;
        _ -> io:format("Actual Redis GET Latency (over TCP): ~8.4f microsec~n", [RedisLatency])
    end,
    case ConsulConfigLatency of
        undefined -> ok;
        _ -> io:format("Actual Consul KV GET Latency (over HTTP): ~8.4f microsec~n", [ConsulConfigLatency])
    end,
    case ConsulDiscLatency of
        undefined -> ok;
        _ -> io:format("Actual Consul Service Discovery Latency (over HTTP): ~8.4f microsec~n", [ConsulDiscLatency])
    end,

    application:stop(sofia),
    application:stop(sasl),
    ok.

loop(0, _) -> ok;
loop(N, Fun) ->
    Fun(),
    loop(N-1, Fun).

start_echo_server() ->
    {ok, ListenSocket} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(ListenSocket),
    Pid = spawn(fun() -> accept_loop(ListenSocket) end),
    {ok, Port, ListenSocket, Pid}.

accept_loop(ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            spawn(fun() -> echo_loop(Socket) end),
            accept_loop(ListenSocket);
        _ ->
            ok
    end.

echo_loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            gen_tcp:send(Socket, Data),
            echo_loop(Socket);
        {error, closed} ->
            ok
    end.

