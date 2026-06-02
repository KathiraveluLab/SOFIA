# SOFIA End-to-End Use Cases

This document demonstrates how to apply SOFIA (Service-Oriented Federated Interoperability Architecture) to build decentralized, high-integrity service policies across municipal, industrial, and social domains, drawing architectural inspiration from the Epicue project.

---

## 1. Use Cases Architecture

SOFIA's decentralized subsystems cooperate to manage data flow, transformation, routing, and transactional consistency for domain-specific registries without central ESB orchestrators:

```
    [ External Sensors / Web Clients ]
                   | (REST / JSON Payload)
                   v
           [ sofia_gateway ]  <--- Resolves target from registry
                   | (Protocol Translation to Erlang Tuple)
                   v
           [ sofia_router ]   <--- Content-based routing
                   | (Selects specific Node/Pid based on payload)
                   v
         [ sofia_transformer ] <--- Schema mapping and normalization
                   v
           [ sofia_breaker ]  <--- Client-side circuit breaker
                   | (Protected call)
                   v
         [ Target Service Pid ] <--- Updates ETS / pg scope
```

---

## 2. Domain 1: Municipal Water Quality System

Tracks potability, pH index metrics, and leak alerts across municipal water works.

### 2.1. Configuration and Thresholds
We define global pH thresholds in the cluster using `sofia_config` so all nodes agree on potability bounds:
```erlang
%% Set pH threshold boundaries (ph_scale = actual_ph * 100)
ok = sofia_config:set(water_ph_min, 650). % pH 6.5
ok = sofia_config:set(water_ph_max, 850). % pH 8.5
```

### 2.2. Protocol Bridging via Gateway
When an external water sensor uploads a JSON-like payload, `sofia_gateway` translates it into a native record tuple:
```erlang
%% External JSON payload mapped to Erlang Map
SensorPayload = #{
    <<"action">> => <<"add_potability_record">>,
    <<"sensor_id">> => <<"sensor_zone_A">>,
    <<"ph">> => 720,
    <<"ppm">> => 150
}.

%% Gateway translates and forwards payload to the registered water service under a circuit breaker
{ok, Result} = sofia_gateway:handle_request(water_service, SensorPayload, water_breaker).
```

---

## 3. Domain 2: Industrial Carbon Traceability & Auditing

Monitors steel mill carbon emissions, verifying compliance with environmental caps.

### 3.1. Dynamic Content-Based Routing
Emissions are routed to specific audit validators depending on the severity of the carbon footprint:
```erlang
%% Dynamic routing function: if carbon emissions exceed 500 tons, route to high-priority auditors
AuditorRouter = fun(Payload, Pids) ->
    Carbon = maps:get(carbon_tons, Payload),
    case Carbon > 500 of
        true -> {ok, lists:last(Pids)}; % Specialized high-capacity auditor
        false -> {ok, lists:first(Pids)} % Standard auditor
    end
end.

AuditPayload = #{mill_id => <<"steel_mill_09">>, carbon_tons => 620},
{ok, TargetAuditorPid} = sofia_router:route(carbon_auditor, AuditPayload, AuditorRouter).
```

### 3.2. Data Transformation and Mapping
Before committing the audit to the database, we use `sofia_transformer` to map legacy telemetry formats to our system's unified schema:
```erlang
TelemetryData = #{
    mill_id => <<"steel_mill_09">>,
    carbon_tons => 620,
    temp_fahrenheit => 2200
}.

%% Map legacy field names to standard audit parameters
SchemaRules = #{
    temp_fahrenheit => temperature_f,
    carbon_tons => co2_tons
}.

NormalizedData = sofia_transformer:transform(TelemetryData, SchemaRules).
%% Returns: #{mill_id => <<"steel_mill_09">>, co2_tons => 620, temperature_f => 2200}
```

---

## 4. Domain 3: Healthcare Outcome Verification

Coordinators use Saga orchestration to manage patient treatment plans across independent clinic registries.

### 4.1. Distributed Saga Transaction
When logging a patient care outcome, the system must (1) reserve clinic capacity, (2) verify insurance clearance, and (3) update the patient outcome ledger. If any step fails, compensation actions roll back the changes to ensure eventual consistency:

```erlang
%% Step 1: Reserve Clinic Bed
ReserveBed = fun() -> {ok, bed_id_104} end,
ReleaseBed = fun(BedId) -> io:format("Releasing bed ~p~n", [BedId]), ok end,

%% Step 2: Clear Insurance (Simulated Failure)
ClearInsurance = fun() -> {error, insufficient_funds} end,
RollbackInsurance = fun(_) -> ok end,

%% Saga Definition
Steps = [
    {ReserveBed, ReleaseBed},
    {ClearInsurance, RollbackInsurance}
],

%% Execute Saga
case sofia_saga:execute(Steps) of
    {ok, Results} ->
        io:format("Patient registered successfully: ~p~n", [Results]);
    {error, {step_failed, Reason, Compensations}} ->
        io:format("Registration failed: ~p. Rollbacks executed: ~p~n", [Reason, Compensations])
end.
%% Output will log the automatic execution of ReleaseBed (compensating Action 1)
```

---

## 5. Domain 4: Higher Education Academic Credentials

Coordinates credential verification and inclusion scoring across university nodes.

### 5.1. Federated Service Discovery
University nodes register their credential verification services. Verification requests query `sofia_registry` to discover and load-balance across available validators:
```erlang
%% Query all available credential verification service nodes
case sofia_registry:discover(credential_verifier) of
    {ok, VerifierPid} ->
        gen_server:call(VerifierPid, {verify_credential, StudentId, DiplomaHash});
    {error, no_service_available} ->
        {error, verifier_offline}
end.
```
