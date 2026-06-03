-module(sofia_rate_limiter).
-behaviour(gen_server).

%% API
-export([start_link/0, check_rate/1, set_sla/3, get_sla/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_RATE, 10.0).      %% tokens/second
-define(DEFAULT_CAPACITY, 10.0).  %% max burst tokens

-record(sofia_slas, {
    client_id,
    rate,
    capacity
}).

%% Distributed bucket state: stored in Mnesia so all nodes share token counts.
%% Each record holds the current token balance and timestamp of last update.
-record(sofia_rate_buckets, {
    client_id,      %% binary client identifier (key)
    tokens,         %% float: current token balance
    last_update     %% erlang:system_time(microsecond)
}).

%% ===================================================================
%% API Functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Atomically check and consume one token for ClientId.
%% Returns ok if admitted, {error, rate_limited} if exhausted.
check_rate(ClientId) ->
    gen_server:call(?SERVER, {check_rate, ClientId}).

set_sla(ClientId, Rate, Capacity) ->
    F = fun() ->
        Record = #sofia_slas{client_id = ClientId, rate = Rate, capacity = Capacity},
        mnesia:write(sofia_slas, Record, write)
    end,
    case mnesia:transaction(F) of
        {atomic, ok} -> ok;
        Other -> Other
    end.

get_sla(ClientId) ->
    F = fun() -> mnesia:read(sofia_slas, ClientId) end,
    case mnesia:transaction(F) of
        {atomic, [#sofia_slas{rate = Rate, capacity = Cap}]} ->
            {Rate, Cap};
        _ ->
            {?DEFAULT_RATE, ?DEFAULT_CAPACITY}
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Ensure the distributed bucket table exists (may already be created by sofia_app)
    ensure_bucket_table(),
    {ok, #{}}.

handle_call({check_rate, ClientId}, _From, State) ->
    {Rate, Capacity} = get_sla(ClientId),
    Now = erlang:system_time(microsecond),
    Reply = mnesia_check_rate(ClientId, Rate, Capacity, Now),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal: Mnesia-backed distributed token bucket
%% ===================================================================

%% Atomic Mnesia transaction: read-refill-deduct-write.
%% Because this runs as a single transaction, it is safe across nodes
%% sharing the same Mnesia replica (disc_copies or ram_copies).
mnesia_check_rate(ClientId, Rate, Capacity, Now) ->
    F = fun() ->
        case mnesia:read(sofia_rate_buckets, ClientId, write) of
            [] ->
                %% First request: initialize with full bucket minus one token
                mnesia:write(sofia_rate_buckets,
                    #sofia_rate_buckets{
                        client_id   = ClientId,
                        tokens      = Capacity - 1.0,
                        last_update = Now
                    }, write),
                ok;
            [#sofia_rate_buckets{tokens = Tokens, last_update = LastUpdate}] ->
                ElapsedSecs = (Now - LastUpdate) / 1_000_000.0,
                Refilled    = erlang:min(Capacity, Tokens + ElapsedSecs * Rate),
                if
                    Refilled >= 1.0 ->
                        mnesia:write(sofia_rate_buckets,
                            #sofia_rate_buckets{
                                client_id   = ClientId,
                                tokens      = Refilled - 1.0,
                                last_update = Now
                            }, write),
                        ok;
                    true ->
                        {error, rate_limited}
                end
        end
    end,
    case mnesia:transaction(F) of
        {atomic, ok}                    -> ok;
        {atomic, {error, rate_limited}} -> {error, rate_limited};
        {aborted, _Reason}              -> ok  %% fail open on DB error
    end.

ensure_bucket_table() ->
    case mnesia:create_table(sofia_rate_buckets,
            [{ram_copies, [node()]},
             {attributes, record_info(fields, sofia_rate_buckets)},
             {type, set}]) of
        {atomic, ok}                          -> ok;
        {aborted, {already_exists, _}}        -> ok;
        _                                     -> ok
    end.
