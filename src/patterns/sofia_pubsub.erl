-module(sofia_pubsub).
-export([subscribe/1, unsubscribe/1, publish/2]).

%% @doc Subscribes the calling process to a federated topic.
subscribe(Topic) ->
    sofia_registry:register_service({topic, Topic}, self()).

%% @doc Unsubscribes the calling process from a federated topic.
unsubscribe(Topic) ->
    sofia_registry:deregister_service({topic, Topic}, self()).

%% @doc Publishes a message directly to all active subscriber Pids.
%% Returns the number of active subscribers that received the message.
publish(Topic, Msg) ->
    case sofia_registry:discover_all({topic, Topic}) of
        [] ->
            0;
        Pids ->
            lists:foreach(fun(Pid) -> Pid ! {sofia_pubsub, Topic, Msg} end, Pids),
            length(Pids)
    end.
