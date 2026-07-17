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

    %% 12. Authentication (SOFIA vs SOTA/Redis)
    TimestampBin = integer_to_binary(erlang:system_time(second)),
    ok = sofia_auth:set_client_secret(<<"bench_client">>, <<"secret_key">>),
    {ok, SigHex} = sofia_auth:sign_payload(<<"bench_client">>, TimestampBin, <<"test_payload">>),
    
    T_auth_sofia_start = erlang:system_time(nanosecond),
    loop(50000, fun() -> 
        ok = sofia_auth:verify_payload(<<"bench_client">>, TimestampBin, SigHex, <<"test_payload">>)
    end),
    T_auth_sofia_end = erlang:system_time(nanosecond),
    AuthSOFIALatency = (T_auth_sofia_end - T_auth_sofia_start) / 50000.0 / 1000.0,

    AuthSOTALatency = case gen_tcp:connect("localhost", 6379, [binary, {active, false}]) of
        {ok, AuthRedisSocket} ->
            ok = gen_tcp:send(AuthRedisSocket, <<"*3\r\n$3\r\nSET\r\n$17\r\nauth:bench_client\r\n$10\r\nsecret_key\r\n">>),
            {ok, _} = gen_tcp:recv(AuthRedisSocket, 0),
            T_auth_sota_start = erlang:system_time(nanosecond),
            loop(5000, fun() ->
                ok = gen_tcp:send(AuthRedisSocket, <<"*2\r\n$3\r\nGET\r\n$17\r\nauth:bench_client\r\n">>),
                {ok, RedisRes} = gen_tcp:recv(AuthRedisSocket, 0),
                Secret = case RedisRes of
                    <<"$", _/binary>> ->
                        binary:part(RedisRes, byte_size(RedisRes) - 12, 10);
                    _ ->
                        <<"secret_key">>
                end,
                DataToSign = <<<<"bench_client">>/binary, ".", TimestampBin/binary, ".", <<"test_payload">>/binary>>,
                ExpectedSig = crypto:mac(hmac, sha256, Secret, DataToSign),
                ExpectedHex = << <<(hex_digit(N)):8>> || <<N:4>> <= ExpectedSig >>,
                SigHex =:= ExpectedHex
            end),
            T_auth_sota_end = erlang:system_time(nanosecond),
            gen_tcp:close(AuthRedisSocket),
            (T_auth_sota_end - T_auth_sota_start) / 5000.0 / 1000.0;
        _ ->
            undefined
    end,

    %% 13. Contract Schema Validation (SOFIA vs SOTA/Consul)
    Schema = #{<<"param1">> => integer, <<"param2">> => binary},
    Contract = #{methods => #{<<"test_method">> => #{input_schema => Schema}}},
    Payload = #{<<"param1">> => 42, <<"param2">> => <<"hello">>},
    ContractJson = jsx:encode(Contract),
    
    T_contract_sofia_start = erlang:system_time(nanosecond),
    loop(50000, fun() ->
        ok = sofia_contract:validate_request(Contract, <<"test_method">>, Payload)
    end),
    T_contract_sofia_end = erlang:system_time(nanosecond),
    ContractSOFIALatency = (T_contract_sofia_end - T_contract_sofia_start) / 50000.0 / 1000.0,

    ContractSOTALatency = case httpc:request(put, {"http://localhost:8500/v1/kv/contracts/bench_service", [], "application/json", ContractJson}, [], []) of
        {ok, _} ->
            T_contract_sota_start = erlang:system_time(nanosecond),
            loop(500, fun() ->
                {ok, {{_, 200, _}, _, Body}} = httpc:request(get, {"http://localhost:8500/v1/kv/contracts/bench_service?raw", []}, [], []),
                Decoded = jsx:decode(list_to_binary(Body), [return_maps]),
                Methods = maps:get(<<"methods">>, Decoded),
                MethodSpec = maps:get(<<"test_method">>, Methods),
                InputSchemaRaw = maps:get(<<"input_schema">>, MethodSpec),
                InputSchema = maps:map(fun(_, V) -> binary_to_existing_atom(V, utf8) end, InputSchemaRaw),
                ContractSpec = #{methods => #{<<"test_method">> => #{input_schema => InputSchema}}},
                ok = sofia_contract:validate_request(ContractSpec, <<"test_method">>, Payload)
            end),
            T_contract_sota_end = erlang:system_time(nanosecond),
            (T_contract_sota_end - T_contract_sota_start) / 500.0 / 1000.0;
        _ ->
            undefined
    end,

    %% 14. QoS-Aware Routing (SOFIA vs SOTA/Consul)
    RoutingPid = spawn(fun L() -> receive stop -> ok; _ -> L() end end),
    ok = sofia_registry:register_service(routing_service, RoutingPid),
    RoutingFun = fun(_P, Pids) -> {ok, hd(Pids)} end,

    T_route_sofia_start = erlang:system_time(nanosecond),
    loop(20000, fun() ->
        {ok, _} = sofia_router:route(routing_service, #{}, RoutingFun)
    end),
    T_route_sofia_end = erlang:system_time(nanosecond),
    RouteSOFIALatency = (T_route_sofia_end - T_route_sofia_start) / 20000.0 / 1000.0,
    sofia_registry:deregister_service(routing_service, RoutingPid),
    exit(RoutingPid, kill),

    RouteSOTALatency = case httpc:request(put, {"http://localhost:8500/v1/agent/service/register", [], "application/json", "{\"ID\": \"routing_service\", \"Name\": \"routing_service\", \"Address\": \"127.0.0.1\", \"Port\": 8080}"}, [], []) of
        {ok, _} ->
            T_route_sota_start = erlang:system_time(nanosecond),
            loop(500, fun() ->
                {ok, {{_, 200, _}, _, Body}} = httpc:request(get, {"http://localhost:8500/v1/health/service/routing_service", []}, [], []),
                Services = jsx:decode(list_to_binary(Body), [return_maps]),
                [FirstService | _] = Services,
                _ServiceNode = maps:get(<<"Service">>, FirstService)
            end),
            T_route_sota_end = erlang:system_time(nanosecond),
            httpc:request(put, {"http://localhost:8500/v1/agent/service/deregister/routing_service", [], "", ""}, [], []),
            (T_route_sota_end - T_route_sota_start) / 500.0 / 1000.0;
        _ ->
            undefined
    end,

    %% 15. Distributed Tracing (SOFIA vs SOTA/HTTP)
    TraceId = sofia_tracer:generate_id(),
    T_trace_sofia_start = erlang:system_time(nanosecond),
    loop(20000, fun() ->
        {ok, SpanId} = sofia_tracer:start_span(TraceId, bench_span, undefined),
        ok = sofia_tracer:end_span(TraceId, SpanId)
    end),
    T_trace_sofia_end = erlang:system_time(nanosecond),
    TraceSOFIALatency = (T_trace_sofia_end - T_trace_sofia_start) / 20000.0 / 1000.0,

    TraceSOTALatency = case httpc:request(get, {"http://localhost:8500/v1/status/leader", []}, [], []) of
        {ok, _} ->
            TracePayload = jsx:encode(#{
                <<"traceId">> => TraceId,
                <<"spanId">> => sofia_tracer:generate_id(),
                <<"name">> => <<"bench_span">>,
                <<"startTime">> => erlang:system_time(microsecond),
                <<"endTime">> => erlang:system_time(microsecond) + 100
            }),
            T_trace_sota_start = erlang:system_time(nanosecond),
            loop(500, fun() ->
                {ok, {{_, _, _}, _, _}} = httpc:request(post, {"http://localhost:8500/v1/kv/traces/bench_span", [], "application/json", TracePayload}, [], [])
            end),
            T_trace_sota_end = erlang:system_time(nanosecond),
            (T_trace_sota_end - T_trace_sota_start) / 500.0 / 1000.0;
        _ ->
            undefined
    end,

    %% 16. Saga Orchestration (SOFIA vs SOTA/Redis)
    ActionFun = fun() -> {ok, result} end,
    CompFun = fun(_) -> ok end,
    Steps = [{ActionFun, CompFun}, {ActionFun, CompFun}],

    T_saga_sofia_start = erlang:system_time(nanosecond),
    loop(20000, fun() ->
        {ok, _} = sofia_saga:execute(Steps)
    end),
    T_saga_sofia_end = erlang:system_time(nanosecond),
    SagaSOFIALatency = (T_saga_sofia_end - T_saga_sofia_start) / 20000.0 / 1000.0,

    SagaSOTALatency = case gen_tcp:connect("localhost", 6379, [binary, {active, false}]) of
        {ok, SagaRedisSocket} ->
            T_saga_sota_start = erlang:system_time(nanosecond),
            loop(5000, fun() ->
                ok = gen_tcp:send(SagaRedisSocket, <<"*3\r\n$3\r\nSET\r\n$8\r\nsaga:123\r\n$7\r\nrunning\r\n">>),
                {ok, _} = gen_tcp:recv(SagaRedisSocket, 0),
                ok = gen_tcp:send(SagaRedisSocket, <<"*4\r\n$4\r\nHSET\r\n$8\r\nsaga:123\r\n$5\r\nstep1\r\n$2\r\nok\r\n">>),
                {ok, _} = gen_tcp:recv(SagaRedisSocket, 0),
                ok = gen_tcp:send(SagaRedisSocket, <<"*4\r\n$4\r\nHSET\r\n$8\r\nsaga:123\r\n$5\r\nstep2\r\n$2\r\nok\r\n">>),
                {ok, _} = gen_tcp:recv(SagaRedisSocket, 0),
                ok = gen_tcp:send(SagaRedisSocket, <<"*3\r\n$3\r\nSET\r\n$8\r\nsaga:123\r\n$9\r\ncompleted\r\n">>),
                {ok, _} = gen_tcp:recv(SagaRedisSocket, 0)
            end),
            T_saga_sota_end = erlang:system_time(nanosecond),
            gen_tcp:close(SagaRedisSocket),
            (T_saga_sota_end - T_saga_sota_start) / 5000.0 / 1000.0;
        _ ->
            undefined
    end,

    io:format("Direct Local Call:                         ~.4f microsec~n", [DirectLatency]),
    io:format("Circuit Breaker Call (Closed):             ~.4f microsec~n", [BreakerLatency]),
    io:format("Registry Service Discovery:                ~.4f microsec~n", [RegistryLatency]),
    io:format("Rate Limiter Check (Mnesia):               ~.4f microsec~n", [RateLimitLatency]),
    io:format("DLQ Enqueue (Mnesia):                      ~.4f microsec~n", [DLQLatency]),
    io:format("Config Update (ETS + RPC):                 ~.4f microsec~n", [ConfigLatency]),
    io:format("SOFIA Auth Verification:                   ~.4f microsec~n", [AuthSOFIALatency]),
    io:format("SOFIA Contract Verification:               ~.4f microsec~n", [ContractSOFIALatency]),
    io:format("SOFIA QoS Dynamic Routing:                 ~.4f microsec~n", [RouteSOFIALatency]),
    io:format("SOFIA Distributed Tracing:                 ~.4f microsec~n", [TraceSOFIALatency]),
    io:format("SOFIA Saga Orchestration:                  ~.4f microsec~n", [SagaSOFIALatency]),

    io:format("Simulated TCP Loopback Roundtrip (SOTA):   ~.4f microsec~n", [TCPRoundtrip]),
    io:format("Simulated HTTP Gateway Roundtrip (SOTA):   ~.4f microsec~n", [HTTPRoundtrip]),
    case RedisLatency of
        undefined -> ok;
        _ -> io:format("Actual Redis GET Latency (SOTA):           ~.4f microsec~n", [RedisLatency])
    end,
    case ConsulConfigLatency of
        undefined -> ok;
        _ -> io:format("Actual Consul KV GET Latency (SOTA):       ~.4f microsec~n", [ConsulConfigLatency])
    end,
    case ConsulDiscLatency of
        undefined -> ok;
        _ -> io:format("Actual Consul Service Discovery (SOTA):    ~.4f microsec~n", [ConsulDiscLatency])
    end,
    case AuthSOTALatency of
        undefined -> ok;
        _ -> io:format("Actual Redis Auth Lookup + HMAC (SOTA):    ~.4f microsec~n", [AuthSOTALatency])
    end,
    case ContractSOTALatency of
        undefined -> ok;
        _ -> io:format("Actual Consul Contract HTTP Fetch (SOTA):  ~.4f microsec~n", [ContractSOTALatency])
    end,
    case RouteSOTALatency of
        undefined -> ok;
        _ -> io:format("Actual Consul QoS Health Route (SOTA):     ~.4f microsec~n", [RouteSOTALatency])
    end,
    case TraceSOTALatency of
        undefined -> ok;
        _ -> io:format("Actual HTTP Trace Collector Post (SOTA):   ~.4f microsec~n", [TraceSOTALatency])
    end,
    case SagaSOTALatency of
        undefined -> ok;
        _ -> io:format("Actual Redis Saga WAL Status Sync (SOTA):  ~.4f microsec~n", [SagaSOTALatency])
    end,

    run_scalability(),

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

hex_digit(N) when N >= 0, N =< 9 -> N + $0;
hex_digit(N) when N >= 10, N =< 15 -> N - 10 + $a.

run_scalability() ->
    Sizes = [2, 4, 8, 16, 32],
    Trials = 50,
    io:format("~nSaga Scalability (Problem Size vs Execution Time):~n"),
    RedisSocket = case gen_tcp:connect("localhost", 6379, [binary, {active, false}]) of
        {ok, Sock} -> Sock;
        _ -> undefined
    end,
    lists:foreach(fun(N) ->
        %% SOFIA Saga
        ActionFun = fun() -> {ok, result} end,
        CompFun = fun(_) -> ok end,
        Steps = [{ActionFun, CompFun} || _ <- lists:seq(1, N)],
        
        SOFIATimes = [
            begin
                T_start = erlang:system_time(nanosecond),
                {ok, _} = sofia_saga:execute(Steps),
                T_end = erlang:system_time(nanosecond),
                (T_end - T_start) / 1000.0 %% microsec
            end || _ <- lists:seq(1, Trials)
        ],
        SOFIAMean = mean(SOFIATimes),
        SOFIAStd = std_dev(SOFIATimes, SOFIAMean),
        
        %% SOTA Saga
        SOTAMean = case RedisSocket of
            undefined -> undefined;
            _ ->
                SOTATimes = [
                    begin
                        T_start2 = erlang:system_time(nanosecond),
                        run_sota_saga(RedisSocket, N),
                        T_end2 = erlang:system_time(nanosecond),
                        (T_end2 - T_start2) / 1000.0 %% microsec
                    end || _ <- lists:seq(1, Trials)
                ],
                SOTAMeanVal = mean(SOTATimes),
                SOTAStdVal = std_dev(SOTATimes, SOTAMeanVal),
                {SOTAMeanVal, SOTAStdVal}
        end,
        
        case SOTAMean of
            undefined ->
                io:format("Steps: ~2b | SOFIA: ~.2f +/- ~.2f us~n", [N, SOFIAMean, SOFIAStd]);
            {SMean, SStd} ->
                io:format("Steps: ~2b | SOFIA: ~.2f +/- ~.2f us | SOTA: ~.2f +/- ~.2f us~n", 
                          [N, SOFIAMean, SOFIAStd, SMean, SStd])
        end
    end, Sizes),
    case RedisSocket of
        undefined -> ok;
        _ -> gen_tcp:close(RedisSocket)
    end,
    ok.

run_sota_saga(Socket, N) ->
    ok = gen_tcp:send(Socket, <<"*3\r\n$3\r\nSET\r\n$8\r\nsaga:123\r\n$7\r\nrunning\r\n">>),
    {ok, _} = gen_tcp:recv(Socket, 0),
    lists:foreach(fun(I) ->
        StepName = list_to_binary("step" ++ integer_to_list(I)),
        LenName = integer_to_binary(byte_size(StepName)),
        Cmd = <<"*4\r\n$4\r\nHSET\r\n$8\r\nsaga:123\r\n$", LenName/binary, "\r\n", StepName/binary, "\r\n$2\r\nok\r\n">>,
        ok = gen_tcp:send(Socket, Cmd),
        {ok, _} = gen_tcp:recv(Socket, 0)
    end, lists:seq(1, N)),
    ok = gen_tcp:send(Socket, <<"*3\r\n$3\r\nSET\r\n$8\r\nsaga:123\r\n$9\r\ncompleted\r\n">>),
    {ok, _} = gen_tcp:recv(Socket, 0),
    ok.

mean(List) ->
    lists:sum(List) / length(List).

std_dev(List, Mean) ->
    DiffSq = [(X - Mean) * (X - Mean) || X <- List],
    math:sqrt(lists:sum(DiffSq) / length(List)).

