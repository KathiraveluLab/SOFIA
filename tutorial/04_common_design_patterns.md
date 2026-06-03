# Chapter 4: Common Design Patterns

SOFIA provides implementations of common decentralized communication patterns out of the box.

## Architecture Diagram

```mermaid
graph TD
    subgraph Pub-Sub (Publish-Subscribe)
        Publisher[Publisher]
        Sub1[Subscriber 1]
        Sub2[Subscriber 2]
        Publisher -.->|direct messaging| Sub1
        Publisher -.->|direct messaging| Sub2
    end
    subgraph Push-Pull Pipeline
        Producer[Producer / Ventilator]
        Worker1[Worker 1]
        Worker2[Worker 2]
        Producer -->|load balanced task| Worker1
        Producer -->|load balanced task| Worker2
    end
    subgraph Scatter-Gather
        Requester[Requester]
        P1[Provider 1]
        P2[Provider 2]
        Requester -->|scatter| P1
        Requester -->|scatter| P2
        P1 -->|gather| Requester
        P2 -->|gather| Requester
    end
```

## 1. Brokerless Publish-Subscribe (Pub-Sub)

Subscribers register directly to topics using `sofia_registry` process groups, allowing publishers to send messages to all active subscribers without an intermediary broker process.

Copy the implementation at [sofia_pubsub.erl](file:///home/pradeeban/SOFIA/src/patterns/sofia_pubsub.erl).

### Subscriber Example
```erlang
%% Subscribe the calling process to the topic
ok = sofia_pubsub:subscribe("market_feed").

%% Wait to receive published messages
receive
    {sofia_pubsub, "market_feed", Msg} ->
        io:format("Received feed update: ~p~n", [Msg])
end.

%% Unsubscribe when done
ok = sofia_pubsub:unsubscribe("market_feed").
```

### Publisher Example
```erlang
%% Publish a message directly to all active subscribers
RecipientsCount = sofia_pubsub:publish("market_feed", {stock_quote, <<"APPL">>, 175.50}).
```

## 2. Brokerless Push-Pull Pipeline

Distributes tasks dynamically across a pool of workers in a pipeline. Workers register to the pipeline, and producers push tasks to workers via local load-balancing discovery.

Copy the implementation at [sofia_pipeline.erl](file:///home/pradeeban/SOFIA/src/patterns/sofia_pipeline.erl).

### Worker Example
```erlang
%% Register the worker to pull tasks from the pipeline
ok = sofia_pipeline:register_worker("job_pipeline").

%% Process incoming tasks
receive
    {sofia_pipeline_task, "job_pipeline", Task} ->
        io:format("Processing pipeline task: ~p~n", [Task])
end.

%% Deregister when stopping
ok = sofia_pipeline:deregister_worker("job_pipeline").
```

### Producer Example
```erlang
%% Push tasks directly to one of the active workers
ok = sofia_pipeline:push_task("job_pipeline", {process_image, <<"image_092.png">>}).
```

## 3. Scatter-Gather Pattern

Broadcasts a request asynchronously to multiple service providers, aggregates all responses within a timeout window, and returns the result. This is highly useful in federated queries or distributed bidding/pricing networks.

Copy the implementation at [sofia_scatter_gather.erl](file:///home/pradeeban/SOFIA/src/patterns/sofia_scatter_gather.erl).

### Responder Example
```erlang
%% Process that registers to quote_provider and responds to scatter requests
receive
    {scatter, Ref, From, {get_quote, Item}} ->
        Price = calculate_price(Item),
        From ! {gather, Ref, {self(), Item, Price}}
end.
```

### Requester Example
```erlang
%% Broadcast quote request to all quote_provider processes and wait up to 1 second
{ok, Quotes} = sofia_scatter_gather:request(quote_provider, {get_quote, <<"widget">>}, 1000).
```

## 4. Resource-Oriented Architecture (ROA) Pattern

Resource-Oriented Architecture (ROA) is a RESTful alternative to traditional service-oriented interfaces. Instead of defining operations on service endpoints, applications manipulate distinct *resources* (each identified by a URI) using a *uniform interface* of messages (`get`, `put`, `post`, `delete`) mapped directly to actors.

Copy the implementation at [sofia_roa.erl](file:///home/pradeeban/SOFIA/src/patterns/sofia_roa.erl).

### Resource Actor Loop Example
```erlang
%% An actor representing a resource (e.g. "/patients/101")
resource_loop(State) ->
    receive
        {roa_request, Ref, From, get} ->
            From ! {roa_response, Ref, {ok, State}},
            resource_loop(State);
        {roa_request, Ref, From, {put, NewState}} ->
            From ! {roa_response, Ref, {ok, NewState}},
            resource_loop(NewState);
        {roa_request, Ref, From, {post, Data}} ->
            NewState = State ++ [Data],
            From ! {roa_response, Ref, {ok, NewState}},
            resource_loop(NewState);
        {roa_request, Ref, From, delete} ->
            From ! {roa_response, Ref, {ok, deleted}},
            ok; % terminate
        stop ->
            ok
    end.
```

### Client Resource Interoperability Example
```erlang
%% 1. Start and register a patient resource
Pid = spawn(fun() -> resource_loop([]) end),
ok = sofia_roa:register_resource("/patients/101", Pid),

%% 2. Perform GET to retrieve representation
{ok, []} = sofia_roa:get("/patients/101"),

%% 3. Perform PUT to set representation
{ok, ["John Doe"]} = sofia_roa:put("/patients/101", ["John Doe"]),

%% 4. Perform POST to update representation
{ok, ["John Doe", "Active"]} = sofia_roa:post("/patients/101", "Active"),

%% 5. Perform DELETE to remove resource
{ok, deleted} = sofia_roa:delete("/patients/101").
```
