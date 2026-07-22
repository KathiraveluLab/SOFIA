# SOFIA Standardized Use Cases

This directory contains executable implementations of the five domain-specific federated service use cases described in the SOFIA research paper. They demonstrate how SOFIA's decentralized actor subsystems replace conventional Enterprise Service Bus (ESB) layers in heterogeneous public environments.

---

## Use Cases Overview

### 1. Municipal Water Quality Monitoring (`water_quality.erl`)
* **Domain Context:** Real-time water potability and pH monitoring across municipal distribution zones.
* **SOFIA Subsystems Tested:**
  * `sofia_config`: Dynamically distributes global safety thresholds (e.g., pH ranges `[6.50, 8.50]`) across cluster nodes without querying a central database.
  * `sofia_gateway`: Handles external JSON sensor payloads, translating them into native Erlang message tuples at node boundaries.
  * `sofia_breaker`: Employs a client-side circuit breaker to isolate high-latency or failing analysis service instances, preventing cascade backpressure.

### 2. Industrial Carbon Traceability & Auditing (`carbon_traceability.erl`)
* **Domain Context:** Strict carbon emissions reporting and steel mill clearances.
* **SOFIA Subsystems Tested:**
  * `sofia_router`: Directs emission logs dynamically based on payload contents (high-emission records `> 500` tons are routed to specialized auditors).
  * `sofia_transformer`: Normalizes legacy telemetry formats (e.g., `temp_fahrenheit` $\rightarrow$ `temperature_f`) in-context to avoid serialization overhead.

### 3. Healthcare Outcome Verification (`healthcare_outcome.erl`)
* **Domain Context:** Inter-clinic patient transfers and treatment log coordination.
* **SOFIA Subsystems Tested:**
  * `sofia_saga`: Coordinates multi-system transactions (clinic capacity reservation $\rightarrow$ insurance clearance verification) using the Saga pattern. If any stage fails (e.g., insurance denied), compensating rollbacks are executed in reverse order, ensuring eventual consistency without distributed locks.

### 4. Higher Education Credentials (`education_credentials.erl`)
* **Domain Context:** Verifying academic diplomas and inclusion scores across autonomous university registries.
* **SOFIA Subsystems Tested:**
  * `sofia_registry`: Registers and discovers available credential validation services.
  * **Attribute-Based Discovery:** Performs context-sensitive metadata searches (e.g., find verifier in region `us-east` running api version `2.1`).
  * **Load Balancing:** Clients load-balance across discovered endpoints randomly to prevent hot-spotting.
  * **Self-Healing Registry:** Automatically evicts stale registry entries using process monitors when a node or service process crashes.

### 5. Healthcare and Finance Portal Interconnection (`portal_interconnection.erl`)
* **Domain Context:** Integrating patient checkout and billing in a healthcare portal with decentralized payment processors.
* **SOFIA Subsystems Tested:**
  * Combines `sofia_gateway`, `sofia_transformer`, `sofia_router`, `sofia_breaker`, and `sofia_saga` in a unified, end-to-end checkout pipeline.

---

## Executing the Use Cases

All use cases are registered under the integration test suite and can be executed via EUnit:

```bash
rebar3 eunit
```

You can also run the use cases manually inside the Erlang shell:

```erlang
$ rebar3 shell
1> usecases_runner:run().
=== Running Municipal Water Quality Monitoring Use Case ===
Water quality: OK

=== Running Industrial Carbon Traceability Use Case ===
Carbon traceability: OK

=== Running Healthcare Outcome Verification Use Case ===
Healthcare outcome: OK

=== Running Higher Education Credentials Use Case ===
Education credentials: OK

=== Running Healthcare and Finance Portal Interconnection Use Case ===
Portal interconnection: OK

All 5 use cases executed successfully!
ok
```

---

## Benchmarking and SOTA Baseline Verification

To truthfully evaluate SOFIA's performance against State-Of-The-Art (SOTA) middleware layers, this folder contains quantitative benchmark scripts, Makefile targets, and reproducibility suites:

### 1. Master Reproducibility Suite (`run_reproducibility_suite.sh`)
Executes unit tests, single-node micro-benchmarks, 2-node clustered benchmarks, and compiles the IEEE TSC manuscript:
```bash
./usecases/run_reproducibility_suite.sh
```

### 2. Convenience Makefile (`Makefile`)
Provides granular targets for running specific verification tasks:
```bash
cd usecases
make check    # Runs full test, benchmark, cluster, and paper compilation suite
make test     # Runs EUnit unit test suite
make bench    # Runs subsystem micro-benchmarks
make cluster  # Runs 2-node clustered deployment benchmark
make paper    # Compiles IEEE TSC manuscript PDF
```

### 3. SOTA Bare-Metal Benchmarks
To execute bare-metal SOTA comparisons against Redis and Consul:
```bash
./usecases/run_sota_benchmarks.sh
```
This script automatically starts the local Redis and Consul servers on the host, runs the Erlang benchmark suite, collects latency metrics, and stops all background SOTA processes cleanly upon completion.

### 4. Docker-based Benchmarks (For Convenience Only)
If you prefer not to compile Redis or download Consul binaries directly on your host machine, you can run the SOTA services inside Docker containers using:
```bash
./usecases/run_sota_benchmarks_docker.sh
```
> [!NOTE]
> The Docker-based runner is provided purely for convenience and accessibility. Because Docker's virtualized networking bridge introduces additional virtualization and network-bridging latency, the official evaluation numbers in the paper are obtained using the bare-metal runner (`run_sota_benchmarks.sh`) to maintain a completely fair hardware baseline comparison.
