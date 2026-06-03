# Chapter 10: Distributed Observability and Tracing

This chapter explains how to monitor, debug, and trace asynchronous service calls across SOFIA boundaries using the built-in **Distributed Tracing Service** (`sofia_tracer`).

---

## 1. Trace Context and Spans

SOFIA implements a lightweight, OpenTelemetry-compliant tracing model based on **Trace Contexts** and **Spans**:
- **Trace ID**: A globally unique identifier (64-bit random hex binary) representing a single end-to-end execution flow.
- **Span ID**: A unique identifier representing a single unit of work (e.g. execution of a specific service in a chain).
- **Parent Span ID**: Links a span to its caller or predecessor, allowing reconstruction of parallel call trees.

---

## 2. The Tracing Service (`sofia_tracer`)

The tracing service runs as a permanent OTP worker process. It maintains a high-performance, concurrent ETS table (`sofia_spans`) to record and query trace metrics.

### Key API Functions:
- `sofia_tracer:generate_id/0`: Generates a unique 64-bit hexadecimal ID.
- `sofia_tracer:start_span(TraceId, SpanName, ParentSpanId)`: Registers the start of a span, recording the microseconds epoch.
- `sofia_tracer:end_span(TraceId, SpanId)`: Marks the completion of a span and calculates the elapsed duration in microseconds.
- `sofia_tracer:get_trace(TraceId)`: Retrieves all spans associated with a trace, sorted chronologically.

---

## 3. Tracing Service Function Chains (SFC)

When a chain is executed through `sofia_orchestrator`, the orchestrator automatically generates a `TraceId` (if not explicitly passed) and traces each step.

```erlang
Chain = [auth_step, validate_step],
{ok, Result, TraceId} = sofia_orchestrator:execute_sfc(Chain, [start], #{}),

%% Retrieve the generated spans
Spans = sofia_tracer:get_trace(TraceId),
lists:foreach(fun(Span) ->
    io:format("Span: ~p, Duration: ~p microseconds~n", 
              [maps:get(name, Span), maps:get(duration, Span)])
end, Spans).
```

---

## 4. Tracing Parallel Workflows

For Directed Hypergraph Workflows, branches can execute in parallel. The orchestrator propagates the parent span context from one node to its destinations. This allows the tracer to record concurrent executions with correct hierarchy parent mappings.

```erlang
%% Execute workflow
{ok, CompletedNodes, TraceId} = sofia_orchestrator:execute_workflow(Workflow, step_a, [start]),

%% Get parallel execution tree
Spans = sofia_tracer:get_trace(TraceId),
io:format("Call Graph: ~p~n", [Spans]).
```
