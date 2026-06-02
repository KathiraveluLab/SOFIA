# SOFIA: Service-Oriented Federated Interoperability Architecture

SOFIA is a lightweight, Erlang-based framework designed for building scalable, fault-tolerant, and distributed service-oriented systems. Unlike traditional Service-Oriented Architecture (SOA), which relies on centralized Enterprise Service Buses (ESB) and heavy-weight orchestration engines (such as BPMN), SOFIA implements federated peer-to-peer interoperability directly at the actor level.

## Core Features

- **Federated Service Registry (`sofia_registry`)**: Fully decentralized service registration and discovery using Erlang process groups (`pg`).
- **Stateful Circuit Breaker (`sofia_breaker`)**: Low-latency, ETS-backed circuit breaker protecting distributed service calls from cascading failures.
- **Federated Configuration Sync (`sofia_config`)**: Decoupled, replicated configuration settings synchronized across nodes via lightweight cluster RPCs.
- **Zero Bloat**: No centralized broker, no complex workflow orchestration engines, and no SOAP/XML overhead.

## Building and Compiling

To compile the SOFIA application, ensure you have Erlang/OTP 27 and `rebar3` installed. Then, run the following command from the root directory:

```bash
rebar3 compile
```

## Running Unit Tests

SOFIA utilizes EUnit to validate its core modules (service registry, circuit breaker transitions, and distributed configuration sync). Run the test suite using:

```bash
rebar3 eunit
```

