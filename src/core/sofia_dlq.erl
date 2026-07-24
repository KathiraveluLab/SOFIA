-module(sofia_dlq).
-behaviour(gen_server).

%% API
-export([start_link/0, enqueue/4, list/0, list/1, get/1, purge/0, prune/0, prune/2, replay/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, sofia_dlq_entries).
-define(DEFAULT_PRUNE_INTERVAL, 60000).      %% 1 minute
-define(DEFAULT_TTL_MS, 604800000).           %% 7 days in ms (7 * 86400 * 1000)
-define(DEFAULT_MAX_ENTRIES, 10000).          %% 10,000 max entries

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

%% @doc Execute an automated pruning cycle (removes expired TTL entries and caps at max_entries).
prune() ->
    gen_server:call(?SERVER, prune).

%% @doc Execute pruning with explicit TTL and MaxEntries bounds.
prune(TTLMs, MaxEntries) ->
    gen_server:call(?SERVER, {prune, TTLMs, MaxEntries}).

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
    PruneInterval = application:get_env(sofia, dlq_prune_interval, ?DEFAULT_PRUNE_INTERVAL),
    TTLMs = application:get_env(sofia, dlq_ttl_ms, ?DEFAULT_TTL_MS),
    MaxEntries = application:get_env(sofia, dlq_max_entries, ?DEFAULT_MAX_ENTRIES),
    
    %% Set up background maintenance timer loop
    {ok, _TimerRef} = timer:send_interval(PruneInterval, prune_tick),
    
    {ok, #{
        prune_interval => PruneInterval,
        ttl_ms         => TTLMs,
        max_entries    => MaxEntries
    }}.

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

handle_call(prune, _From, State = #{ttl_ms := TTLMs, max_entries := MaxEntries}) ->
    Res = prune_expired_and_overflow(TTLMs, MaxEntries),
    {reply, Res, State};
handle_call({prune, TTLMs, MaxEntries}, _From, State) ->
    Res = prune_expired_and_overflow(TTLMs, MaxEntries),
    {reply, Res, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_info(prune_tick, State = #{ttl_ms := TTLMs, max_entries := MaxEntries}) ->
    _ = prune_expired_and_overflow(TTLMs, MaxEntries),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal Helpers
%% ===================================================================

prune_expired_and_overflow(TTLMs, MaxEntries) ->
    NowMs = erlang:system_time(millisecond),
    Cutoff = NowMs - TTLMs,
    F = fun() ->
        AllEntries = mnesia:match_object(#sofia_dlq_entries{_ = '_'}),
        %% 1. Remove expired entries older than TTL
        {Expired, Valid} = lists:partition(fun(#sofia_dlq_entries{timestamp = Ts}) -> Ts < Cutoff end, AllEntries),
        lists:foreach(fun(#sofia_dlq_entries{entry_id = Id}) -> mnesia:delete({?TABLE, Id}) end, Expired),
        
        %% 2. Enforce capacity limit if valid count exceeds MaxEntries
        SortedValid = lists:keysort(#sofia_dlq_entries.timestamp, Valid),
        Count = length(SortedValid),
        DroppedOverflow = if
            Count > MaxEntries ->
                ToDropCount = Count - MaxEntries,
                {ToDrop, _Keep} = lists:split(ToDropCount, SortedValid),
                lists:foreach(fun(#sofia_dlq_entries{entry_id = Id}) -> mnesia:delete({?TABLE, Id}) end, ToDrop),
                ToDropCount;
            true ->
                0
        end,
        {length(Expired), DroppedOverflow}
    end,
    case mnesia:transaction(F) of
        {atomic, Res} -> {ok, Res};
        Other        -> Other
    end.

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

