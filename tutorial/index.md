# SOFIA Tutorial Series: Federated Service Orchestration

Welcome to the SOFIA step-by-step tutorial series. This documentation is structured to guide you from building your first federated service to implementing advanced communication patterns in a decentralized environment.

## Tutorial Chapters

1. **[Chapter 1: Creating and Registering Services](01_creating_and_registering_services.md)**
   Learn how to build a standard Erlang service and register it with SOFIA's decentralized registry.
   
2. **[Chapter 2: Invoking Services with Circuit Breaker Protection](02_invoking_services_with_circuit_breaker.md)**
   Understand how to discover active services and shield your invocations against failures using client-side circuit breakers.

3. **[Chapter 3: Distributed Configuration and Swarms](03_distributed_configuration_and_swarms.md)**
   Set up local multi-node clusters (swarms), connect nodes, and observe configuration synchronization in real-time.

4. **[Chapter 4: Common Design Patterns](04_common_design_patterns.md)**
   Explore out-of-the-box implementations for brokerless Publish-Subscribe (Pub-Sub), Push-Pull Pipelines, and Scatter-Gather.

5. **[Chapter 5: Advanced Federation Patterns](05_advanced_federation_patterns.md)**
   Understand service-level multitenancy isolation with fallback routing, and decentralized service function chaining (SFC).

6. **[Chapter 6: Hypergraph Service Workflows](06_hypergraph_service_workflows.md)**
   Define, parse, and execute service workflows in the form of directed hypergraphs using YAML notation.

7. **[Chapter 7: Self-Describing Federated Services](07_self_describing_services.md)**
   Implement service contract registry mappings and perform client-side schema verification prior to actor invocation.

8. **[Chapter 8: External Edge Gateway with Cowboy](08_external_edge_gateway.md)**
   Expose federated services externally via HTTP/JSON API endpoints, complete with dynamic schema validation and error mapping.

9. **[Chapter 9: Supervised Orchestration and Sagas](09_supervised_orchestration_and_sagas.md)**
   Implement process supervision, state-recovery timers, and compensating sagas inside SFC and Hypergraph Workflows.

10. **[Chapter 10: Distributed Observability and Tracing](10_distributed_observability_and_tracing.md)**
    Monitor execution latency and reconstruct call trees using distributed traces and spans.


