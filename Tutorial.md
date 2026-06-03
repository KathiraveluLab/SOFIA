# SOFIA Tutorial: Building and Using Federated Services

This tutorial guides you through the practical steps to build, register, invoke, and configure federated services using the SOFIA framework.

## 1. Creating and Registering a Service

To participate in the SOFIA federation, a service runs as an Erlang process and registers itself with `sofia_registry`.

> [!TIP]
> A complete, compileable service template is provided in [sofia_service_skeleton.erl](file:///home/pradeeban/SOFIA/src/core/sofia_service_skeleton.erl). Developers can copy and extend this skeleton to implement their custom services.

Below is an example of a simple calculator service (`calc_service.erl`) implemented using the standard Erlang `gen_server` behavior:

```erlang
-module(calc_service).
-behaviour(gen_server).

%% API
-export([start_link/0, add/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

add(Pid, A, B) ->
    gen_server:call(Pid, {add, A, B}).

init([]) ->
    %% Register this service process under the service type 'calculator'
    ok = sofia_registry:register_service(calculator, self()),
    {ok, unused_state}.

handle_call({add, A, B}, _From, State) ->
    {reply, {ok, A + B}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Clean up registration upon termination
    ok = sofia_registry:deregister_service(calculator, self()),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
```

## 2. Invoking a Service with Circuit Breaker Protection

When calling external or distributed services, use `sofia_breaker:call/2` (or `sofia_breaker:call/3` with options) to prevent cascading failures.

> [!TIP]
> A reusable service client stub is provided in [sofia_client_stub.erl](file:///home/pradeeban/SOFIA/src/core/sofia_client_stub.erl). Developers can use or extend this client stub to invoke SOFIA services with built-in circuit breaker protection.

Here is how a client discovers the calculator service and invokes it safely:

```erlang
-module(client_example).
-export([execute_addition/2]).

execute_addition(A, B) ->
    %% 1. Discover a service instance from the federated registry
    case sofia_registry:discover(calculator) of
        {error, no_service_available} ->
            {error, service_not_found};
        {ok, ServicePid} ->
            %% Define the invocation function
            InvocationFun = fun() ->
                calc_service:add(ServicePid, A, B)
            end,
            
            %% 2. Invoke the service through the circuit breaker
            %% This protects the caller if the calculator process crashes or hangs.
            %% You can pass custom thresholds: max_failures and reset_timeout (in ms)
            sofia_breaker:call(calculator_breaker, InvocationFun, #{
                max_failures => 3,
                reset_timeout => 10000
            })
    end.
```

If the calculator service fails repeatedly, the circuit breaker trips. Subsequent calls to `sofia_breaker:call` will fail immediately with `{error, circuit_open}` without hitting the remote service, saving resources.

## 3. Distributed Configuration

SOFIA syncs settings across all connected nodes automatically. When you update a setting on one node, it broadcasts to all other nodes in the Erlang cluster.

### Writing a Configuration Setting
On Node A:
```erlang
%% This sets the value locally and broadcasts it to all other connected nodes
sofia_config:set(request_timeout, 5000).
```

### Reading a Configuration Setting
On Node B:
```erlang
%% Retrieves the value locally from the replicated ETS table in O(1) time
Timeout = sofia_config:get(request_timeout, 3000).
```

## 4. Running a Local Multi-Node Swarm

To see the federated registry and configuration sync in action across multiple Erlang nodes on your local machine:

1. Start the first node:
   ```bash
   rebar3 shell --name node1@127.0.0.1 --setcookie sofia_cookie
   ```

2. In a separate terminal, start the second node:
   ```bash
   rebar3 shell --name node2@127.0.0.1 --setcookie sofia_cookie
   ```

3. Connect the nodes together (run this on Node 1):
   ```erlang
   net_adm:ping('node2@127.0.0.1').
   %% Returns: pong
   ```

4. Verify federated configuration sync:
   - On Node 1, run:
     ```erlang
     sofia_config:set(encryption_key, "secret-key-123").
     ```
   - On Node 2, verify the config has automatically synchronized:
     ```erlang
     sofia_config:get(encryption_key).
     %% Returns: "secret-key-123"
     ```

5. Verify federated service discovery:
   - On Node 2, start and register a calculator service:
     ```erlang
     {ok, Pid} = calc_service:start_link().
     ```
   - On Node 1, discover and call the service registered on Node 2:
     ```erlang
     {ok, RemotePid} = sofia_registry:discover(calculator).
     sofia_breaker:call(calc_breaker, fun() -> calc_service:add(RemotePid, 40, 2) end).
     %% Returns: {ok, 42}
     ```
