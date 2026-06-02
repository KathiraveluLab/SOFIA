-module(sofia_breaker).
-behaviour(gen_server).

%% API
-export([start_link/0, call/2, call/3, get_state/1, reset/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, sofia_breakers_table).
-define(DEFAULT_MAX_FAILURES, 3).
-define(DEFAULT_RESET_TIMEOUT, 5000). %% 5 seconds in ms

-record(state, {}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

call(Service, Fun) ->
    call(Service, Fun, #{}).

call(Service, Fun, Opts) ->
    MaxFailures = maps:get(max_failures, Opts, ?DEFAULT_MAX_FAILURES),
    ResetTimeout = maps:get(reset_timeout, Opts, ?DEFAULT_RESET_TIMEOUT),
    case check_circuit(Service, ResetTimeout) of
        closed ->
            execute_call(Service, Fun, MaxFailures);
        half_open ->
            execute_half_open(Service, Fun, MaxFailures);
        open ->
            {error, circuit_open}
    end.

get_state(Service) ->
    case ets:lookup(?TABLE, Service) of
        [{Service, State, _Failures, _LastFailTime}] ->
            {ok, State};
        [] ->
            {ok, closed}
    end.

reset(Service) ->
    gen_server:call(?SERVER, {reset, Service}).

%% ===================================================================
%% Internal Helpers
%% ===================================================================

check_circuit(Service, ResetTimeout) ->
    case ets:lookup(?TABLE, Service) of
        [] ->
            ets:insert(?TABLE, {Service, closed, 0, 0}),
            closed;
        [{Service, closed, _, _}] ->
            closed;
        [{Service, half_open, _, _}] ->
            half_open;
        [{Service, open, Failures, LastFailTime}] ->
            Now = erlang:system_time(millisecond),
            if
                (Now - LastFailTime) >= ResetTimeout ->
                    %% Transition to half-open to test the service
                    ets:insert(?TABLE, {Service, half_open, Failures, LastFailTime}),
                    half_open;
                true ->
                    open
            end
    end.

execute_call(Service, Fun, MaxFailures) ->
    try Fun() of
        {error, Reason} ->
            register_failure(Service, MaxFailures),
            {error, Reason};
        Result ->
            %% If it returns a standard tuple like {ok, Val} or just Val
            Result
    catch
        Class:Reason:Stacktrace ->
            register_failure(Service, MaxFailures),
            erlang:raise(Class, Reason, Stacktrace)
    end.

execute_half_open(Service, Fun, MaxFailures) ->
    try Fun() of
        {error, Reason} ->
            register_failure(Service, MaxFailures),
            {error, Reason};
        Result ->
            register_success(Service),
            Result
    catch
        Class:Reason:Stacktrace ->
            register_failure(Service, MaxFailures),
            erlang:raise(Class, Reason, Stacktrace)
    end.

register_failure(Service, MaxFailures) ->
    gen_server:call(?SERVER, {failure, Service, MaxFailures}).

register_success(Service) ->
    gen_server:call(?SERVER, {success, Service}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% ETS table is public so reads (check_circuit) are fast and concurrent
    ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    {ok, #state{}}.

handle_call({reset, Service}, _From, State) ->
    ets:insert(?TABLE, {Service, closed, 0, 0}),
    {reply, ok, State};

handle_call({failure, Service, MaxFailures}, _From, State) ->
    Now = erlang:system_time(millisecond),
    case ets:lookup(?TABLE, Service) of
        [{Service, closed, Failures, _}] ->
            NewFailures = Failures + 1,
            if
                NewFailures >= MaxFailures ->
                    ets:insert(?TABLE, {Service, open, NewFailures, Now});
                true ->
                    ets:insert(?TABLE, {Service, closed, NewFailures, 0})
            end;
        [{Service, half_open, Failures, _}] ->
            %% Any failure in half-open immediately opens the circuit again
            ets:insert(?TABLE, {Service, open, Failures + 1, Now});
        [{Service, open, Failures, _}] ->
            %% Keep it open, update last failure time
            ets:insert(?TABLE, {Service, open, Failures, Now});
        [] ->
            ets:insert(?TABLE, {Service, closed, 1, 0})
    end,
    {reply, ok, State};

handle_call({success, Service}, _From, State) ->
    ets:insert(?TABLE, {Service, closed, 0, 0}),
    {reply, ok, State};

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
