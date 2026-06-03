-module(sofia_rate_limiter).
-behaviour(gen_server).

%% API
-export([start_link/0, check_rate/1, set_sla/3, get_sla/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    buckets = #{} :: map() %% ClientId -> {Tokens, LastUpdateTime}
}).

-record(sofia_slas, {
    client_id,
    rate,
    capacity
}).

%% ===================================================================
%% API Functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
            %% Default fallback SLA: 10 requests per second, capacity of 10
            {10.0, 10.0}
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #state{buckets = #{}}}.

handle_call({check_rate, ClientId}, _From, State) ->
    {Rate, Capacity} = get_sla(ClientId),
    Now = erlang:system_time(microsecond),
    Buckets = State#state.buckets,
    {Reply, NewBuckets} = case maps:find(ClientId, Buckets) of
        error ->
            %% First request: charge 1 token, initialize bucket
            {ok, maps:put(ClientId, {Capacity - 1.0, Now}, Buckets)};
        {ok, {Tokens, LastUpdate}} ->
            ElapsedSecs = (Now - LastUpdate) / 1000000.0,
            NewTokens = erlang:min(Capacity, Tokens + ElapsedSecs * Rate),
            if
                NewTokens >= 1.0 ->
                    {ok, maps:put(ClientId, {NewTokens - 1.0, Now}, Buckets)};
                true ->
                    {{error, rate_limited}, Buckets}
            end
    end,
    {reply, Reply, State#state{buckets = NewBuckets}};

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
