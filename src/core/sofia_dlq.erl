-module(sofia_dlq).
-behaviour(gen_server).

%% API
-export([start_link/0, enqueue/4, list/0, list/1, get/1, purge/0, replay/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, sofia_dlq_entries).

-record(sofia_dlq_entries, {
    entry_id,       %% unique binary id (UUID-like)
    timestamp,      %% erlang:system_time(millisecond)
    service,        %% service atom targeted
    reason,         %% rejection reason atom/term
    payload,        %% original payload binary or map
    client_id,      %% binary client identifier
    node            %% node() where rejection occurred
}).

%% ===================================================================
%% API Functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Enqueue a rejected message into the Dead-Letter Queue.
enqueue(Service, Reason, Payload, ClientId) ->
    gen_server:cast(?SERVER, {enqueue, Service, Reason, Payload, ClientId}).

%% @doc List all DLQ entries.
list() ->
    F = fun() -> mnesia:match_object(#sofia_dlq_entries{_ = '_'}) end,
    case mnesia:transaction(F) of
        {atomic, Entries} -> {ok, [entry_to_map(E) || E <- Entries]};
        _ -> {ok, []}
    end.

%% @doc List DLQ entries filtered by service name.
list(Service) ->
    F = fun() ->
        Pattern = #sofia_dlq_entries{service = Service, _ = '_'},
        mnesia:match_object(Pattern)
    end,
    case mnesia:transaction(F) of
        {atomic, Entries} -> {ok, [entry_to_map(E) || E <- Entries]};
        _ -> {ok, []}
    end.

%% @doc Get a single DLQ entry by its ID.
get(EntryId) ->
    F = fun() -> mnesia:read(?TABLE, EntryId) end,
    case mnesia:transaction(F) of
        {atomic, [Entry]} -> {ok, entry_to_map(Entry)};
        {atomic, []}      -> {error, not_found};
        _                 -> {error, db_error}
    end.

%% @doc Purge all entries from the DLQ.
purge() ->
    F = fun() ->
        Keys = mnesia:all_keys(?TABLE),
        lists:foreach(fun(K) -> mnesia:delete({?TABLE, K}) end, Keys)
    end,
    case mnesia:transaction(F) of
        {atomic, ok} -> ok;
        _            -> {error, purge_failed}
    end.

%% @doc Replay a DLQ entry by re-routing it through sofia_client_stub.
replay(EntryId) ->
    case sofia_dlq:get(EntryId) of
        {ok, #{service := Service, payload := Payload}} ->
            ServiceAtom = if is_atom(Service) -> Service;
                             is_binary(Service) -> binary_to_existing_atom(Service, utf8)
                          end,
            %% Best-effort replay; caller can observe result
            sofia_client_stub:call_service(ServiceAtom, Payload);
        {error, _} = Err ->
            Err
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #{}}.

handle_cast({enqueue, Service, Reason, Payload, ClientId}, State) ->
    EntryId = generate_id(),
    Record = #sofia_dlq_entries{
        entry_id  = EntryId,
        timestamp = erlang:system_time(millisecond),
        service   = Service,
        reason    = Reason,
        payload   = Payload,
        client_id = ClientId,
        node      = node()
    },
    F = fun() -> mnesia:write(?TABLE, Record, write) end,
    mnesia:transaction(F),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal Helpers
%% ===================================================================

generate_id() ->
    %% Collision-resistant ID: node + monotonic time + random suffix
    Base = io_lib:format("~p-~p-~p", [node(), erlang:monotonic_time(), rand:uniform(1000000)]),
    list_to_binary(Base).

entry_to_map(#sofia_dlq_entries{
    entry_id  = Id,
    timestamp = Ts,
    service   = Svc,
    reason    = Reason,
    payload   = Payload,
    client_id = ClientId,
    node      = Node
}) ->
    #{
        entry_id  => Id,
        timestamp => Ts,
        service   => Svc,
        reason    => Reason,
        payload   => Payload,
        client_id => ClientId,
        node      => Node
    }.
