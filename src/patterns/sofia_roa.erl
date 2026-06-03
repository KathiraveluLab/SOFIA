-module(sofia_roa).
-export([register_resource/2, deregister_resource/2, discover_resource/1]).
-export([get/1, put/2, post/2, delete/1]).

%% @doc Registers a resource actor by its URI.
register_resource(URI, Pid) ->
    sofia_registry:register_service({resource, URI}, Pid).

%% @doc Deregisters a resource actor by its URI.
deregister_resource(URI, Pid) ->
    sofia_registry:deregister_service({resource, URI}, Pid).

%% @doc Discovers a resource actor by its URI.
discover_resource(URI) ->
    sofia_registry:discover({resource, URI}).

%% @doc GET representation of a resource.
get(Pid) when is_pid(Pid) ->
    call_resource(Pid, get);
get(URI) ->
    case discover_resource(URI) of
        {ok, Pid} -> call_resource(Pid, get);
        {error, Reason} -> {error, Reason}
    end.

%% @doc PUT data into a resource.
put(Pid, Data) when is_pid(Pid) ->
    call_resource(Pid, {put, Data});
put(URI, Data) ->
    case discover_resource(URI) of
        {ok, Pid} -> call_resource(Pid, {put, Data});
        {error, Reason} -> {error, Reason}
    end.

%% @doc POST data to a resource.
post(Pid, Data) when is_pid(Pid) ->
    call_resource(Pid, {post, Data});
post(URI, Data) ->
    case discover_resource(URI) of
        {ok, Pid} -> call_resource(Pid, {post, Data});
        {error, Reason} -> {error, Reason}
    end.

%% @doc DELETE a resource.
delete(Pid) when is_pid(Pid) ->
    call_resource(Pid, delete);
delete(URI) ->
    case discover_resource(URI) of
        {ok, Pid} -> call_resource(Pid, delete);
        {error, Reason} -> {error, Reason}
    end.

%% Internal helper to invoke synchronous call
call_resource(Pid, Action) ->
    Ref = make_ref(),
    Self = self(),
    Pid ! {roa_request, Ref, Self, Action},
    receive
        {roa_response, Ref, Reply} ->
            Reply
    after 2000 ->
        {error, timeout}
    end.
