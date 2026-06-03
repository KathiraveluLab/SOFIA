# Chapter 9: Supervised Orchestration and Sagas

This chapter explains how to execute Service Function Chains (SFC) and Directed Hypergraph Workflows using the **Supervised Orchestrator** (`sofia_orchestrator`). It details recovery policies (retries) and how to configure and execute **Compensating Saga Transactions** to roll back partial executions in the event of step failure.

---

## 1. The Need for Supervised Orchestration

In decentralized SFCs and workflows, execution is routed asynchronously from one service node to the next. If a service node crashes, hangs, or experiences network disruption mid-execution, the chain breaks silently. 

The `sofia_orchestrator` acts as a coordinator process that:
1. Listens to real-time progress notifications (`sfc_progress` / `workflow_progress`).
2. Monitors active execution states and maintains a ledger of completed steps.
3. Detects timeouts if a service fails to respond or forward within the configured window.
4. Triggers **Recovery Policies** (such as dynamic retries or compensating sagas).

---

## 2. Configuring Compensating Sagas

A compensating saga recovers from failure by executing undo (compensating) actions in reverse order for all successfully completed steps.

### Defining Compensations in Service Contracts
To register a compensating action, include a `compensations` map in the service contract:

```erlang
Contract = #{
    compensations => #{
        do_auth => undo_auth
    }
}.
ok = sofia_registry:register_service(auth_step, AuthPid, Contract).
```

If the orchestrator needs to compensate `auth_step` after a successful `do_auth` execution, it automatically looks up the contract and executes the `undo_auth` method, passing the payload that was present when `do_auth` completed.

---

## 3. Orchestrated Service Function Chaining (SFC)

To execute a monitored SFC chain with saga recovery:

```erlang
Chain = [auth_step, validate_step, billing_step],
InitialPayload = [start],
Options = #{
    policy => saga,   %% Or 'retry'
    timeout => 2000   %% Timeout in milliseconds per step
},

case sofia_orchestrator:execute_sfc(Chain, InitialPayload, Options) of
    {ok, FinalResult} ->
        io:format("SFC completed: ~p~n", [FinalResult]);
    {error, {saga_rolled_back, {failed_at, billing_step, Reason}}} ->
        io:format("Billing failed. Compensating transactions executed.~n")
end.
```

---

## 4. Orchestrated Hypergraph Workflows

Directed hypergraph workflows (defined using YAML) can also be orchestrated. The coordinator calculates the expected leaf nodes of the hypergraph and terminates only when all branches have successfully reached their leaf targets. If any branch fails, all completed nodes are rolled back.

```erlang
Yaml = "
name: payment_workflow
- source: auth_node
  destinations:
    - billing_node
",
{ok, Workflow} = sofia_workflow:parse_yaml(Yaml),

case sofia_orchestrator:execute_workflow(Workflow, auth_node, [start]) of
    {ok, CompletedNodes} ->
        io:format("Workflow completed: ~p~n", [CompletedNodes]);
    {error, {failed_at, billing_node, Reason}} ->
        io:format("Workflow failed and was rolled back.~n")
end.
```
