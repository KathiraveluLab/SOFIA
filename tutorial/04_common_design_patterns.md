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
