# Chapter 5: Advanced Federation Patterns

SOFIA supports advanced architectural patterns required in complex enterprise environments: service-level multitenancy isolation and decentralized service function chaining.

## 1. Abstracting Multitenancy at the Service Level

In a federated swarm, multiple tenants share infrastructure but require isolated services. SOFIA implements tenant-scoped service registries, allowing tenants to register specialized instances. If a tenant-specific instance is not available, the registry automatically routes the request to a shared global fallback instance.

### Multitenancy Routing Flow

```mermaid
graph TD
    Client[Client Request] --> Registry{Registry Lookup}
    Registry -->|Tenant A Registered| ServiceA[Tenant A Service]
    Registry -->|Tenant B Not Registered| ServiceGlobal[Global Shared Service]
```

### Code Example

Copy the implementation at [sofia_multitenant.erl](file:///home/pradeeban/SOFIA/src/patterns/sofia_multitenant.erl).

#### Registering Services
```erlang
%% Register a dedicated service instance for Tenant A
ok = sofia_multitenant:register_tenant_service(tenant_A, calc_service, TenantAPid).

%% Register a shared global fallback instance
ok = sofia_multitenant:register_tenant_service(global, calc_service, GlobalPid).
```

#### Discovering Services
```erlang
%% Returns TenantAPid
{ok, Pid1} = sofia_multitenant:discover_tenant_service(tenant_A, calc_service).

%% Tenant B is not registered specifically; automatically falls back to GlobalPid
{ok, Pid2} = sofia_multitenant:discover_tenant_service(tenant_B, calc_service).
```

---

## 2. Service Function Chaining (SFC)

Service Function Chaining (SFC) enables routing messages through a ordered sequence of virtualized network functions or services (e.g. `[auth_step, validate_step, log_step]`). 

SOFIA uses a **fully decentralized choreography-based SFC** pattern. The chain path is carried in the message envelope. Each node in the chain performs its local processing, pops itself off the chain list, discovers the next link in the chain dynamically via the registry, and forwards the message. The final node directly returns the result to the originator client.

### SFC Flow Diagram

```mermaid
sequenceDiagram
    participant C as Originator Client
    participant S1 as Auth Step (auth_step)
    participant S2 as Validator Step (validate_step)
    participant S3 as Logger Step (log_step)
    
    C->>S1: start_chain([auth, validate, log], Payload)
    S1->>S2: forward_chain([validate, log], NewPayload)
    S2->>S3: forward_chain([log], NewPayload)
    S3->>C: forward_chain([], FinalPayload) -> sfc_complete
```

### Code Example

Copy the implementation at [sofia_sfc.erl](file:///home/pradeeban/SOFIA/src/patterns/sofia_sfc.erl).

#### Step Service Loop
```erlang
%% Standard process loop for a service participating in chains
service_loop() ->
    receive
        {sfc_step, RemainingChain, Payload, Originator} ->
            %% Process data locally
            ProcessedPayload = process_payload(Payload),
            %% Forward down the chain
            ok = sofia_sfc:forward_chain(RemainingChain, ProcessedPayload, Originator),
            service_loop();
        stop -> ok
    end.
```

#### Initiating a Chain
```erlang
%% Start the chain execution from the client context
Chain = [auth_step, validate_step, log_step],
ok = sofia_sfc:start_chain(Chain, InitialPayload, self()),

%% Receive the final result
receive
    {sfc_complete, FinalPayload} ->
        io:format("Chain execution complete: ~p~n", [FinalPayload]);
    {sfc_error, {Service, Reason}} ->
        io:format("Chain failed at ~p: ~p~n", [Service, Reason])
end.
```

---

## 3. QoS-Aware Adaptive Routing (`sofia_router`)

In federated swarm deployments, service endpoints may exhibit heterogeneous performance due to varying network conditions or node loads. SOFIA provides **QoS-aware adaptive routing** via [sofia_router.erl](file:///c:/Users/pkathiravelu/SOFIA/src/core/sofia_router.erl) to dynamically select the healthiest instance without requiring external sidecar proxies (like Envoy or Istio).

### QoS Multi-Dimensional Metric Ranking

Before evaluating any caller-supplied routing function $R$, `sofia_router` pre-filters and sorts candidate `Pids` using a multi-dimensional metric tuple:

$$\text{QoSKey}(Pid) = (\text{BreakerState}(Pid), \text{MailboxLen}(Pid), \text{AvgLatency}(Pid))$$

1. **Stateful Circuit Breaker (`BreakerState`)**: Endpoints on nodes with `open` circuit breakers are discarded immediately. Remaining instances are assigned priorities (`closed = 1 < half_open = 2`).
2. **Process Mailbox Depth (`MailboxLen`)**: Candidate processes with mailbox queue lengths exceeding `max_mailbox_size` (default `100`) are shed with `{error, overloaded}`. Remaining endpoints are ordered by ascending message queue length.
3. **Historical Telemetry Latency (`AvgLatency`)**: Average execution duration (in microseconds) calculated directly from completed tracing spans stored in Mnesia by `sofia_tracer`.

### Code Example: Content & QoS Routing

```erlang
%% Define a routing selection function (e.g. choose first healthy endpoint)
RouteFun = fun(_Payload, QoSSortedPids) -> 
    {ok, hd(QoSSortedPids)} 
end,

%% Route request through sofia_router
case sofia_router:route(payment_processor, #{method => <<"stripe">>}, RouteFun) of
    {ok, SelectedPid} ->
        gen_server:call(SelectedPid, process_payment);
    {error, overloaded} ->
        io:format("All service mailboxes are congested!~n");
    {error, no_service_available} ->
        io:format("No healthy payment service instances found.~n")
end.
```

