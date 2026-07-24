-module(sofia_saga).
-behaviour(gen_server).

%% API
-export([start_link/0, execute/1, execute/2, recover_sagas/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_STEP_TIMEOUT, 5000). %% 5 seconds default per step
-define(MAX_RECOVERY_ATTEMPTS, 3).   %% Max recovery attempts before marking saga failed

-record(state, {}).

-record(sofia_sagas, {
    saga_id,
    status, %% running | completed | rolling_back | rolled_back | failed
    completed_steps = [], %% list of {Index, Result}
    total_steps = 0,
    steps = [],
    retry_count = 0
}).


%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Executes a list of steps in a Saga orchestration using default step timeout (5000 ms).
%% Each step is of form: {Action, Compensate}
%% Action: 0-arity fun or {M, F, A}
%% Compensate: 1-arity fun receiving the Action's result, or {M, F, A}
execute(Steps) ->
    execute(Steps, ?DEFAULT_STEP_TIMEOUT).

%% @doc Executes a list of steps with a specified step execution timeout (in ms) or Options map.
%% Options map syntax: #{timeout => TimeoutMs}.
%% If any step exceeds the timeout, execution halts and compensating rollbacks execute in reverse order.
execute(Steps, TimeoutMs) when is_integer(TimeoutMs) ->

    SagaId = make_ref(),
    TotalSteps = length(Steps),
    Record = #sofia_sagas{
        saga_id = SagaId,
        status = running,
        completed_steps = [],
        total_steps = TotalSteps,
        steps = Steps
    },
    F = fun() -> mnesia:write(Record) end,
    {atomic, ok} = mnesia:transaction(F),
    execute_loop(SagaId, Steps, 1, [], TimeoutMs);
execute(Steps, Opts) when is_map(Opts) ->
    TimeoutMs = maps:get(timeout, Opts, ?DEFAULT_STEP_TIMEOUT),
    execute(Steps, TimeoutMs).

%% ===================================================================
%% Internal Helpers
%% ===================================================================

execute_loop(SagaId, [], _Index, Completed, _TimeoutMs) ->
    F = fun() ->
        case mnesia:read(sofia_sagas, SagaId) of
            [R] ->
                mnesia:write(R#sofia_sagas{status = completed});
            [] ->
                ok
        end
    end,
    {atomic, _} = mnesia:transaction(F),
    Results = [Res || {_Idx, Res, _Comp} <- lists:reverse(Completed)],
    {ok, Results};
execute_loop(SagaId, [{Action, Compensate} | Rest], Index, Completed, TimeoutMs) ->
    case run_action_with_timeout(Action, TimeoutMs) of
        {ok, Result} ->
            NewCompleted = [{Index, Result, Compensate} | Completed],
            F = fun() ->
                case mnesia:read(sofia_sagas, SagaId) of
                    [R] ->
                        mnesia:write(R#sofia_sagas{completed_steps = [{Idx, Res} || {Idx, Res, _} <- NewCompleted]});
                    [] ->
                        ok
                end
            end,
            {atomic, _} = mnesia:transaction(F),
            execute_loop(SagaId, Rest, Index + 1, NewCompleted, TimeoutMs);
        {error, Reason} ->
            CompensateResults = rollback_saga(SagaId, Completed),
            {error, {step_failed, Reason, CompensateResults}};
        {crashed, Class, Reason, Stacktrace} ->
            CompensateResults = rollback_saga(SagaId, Completed),
            {error, {step_crashed, Class, Reason, Stacktrace, CompensateResults}};
        Other ->
            NewCompleted = [{Index, Other, Compensate} | Completed],
            F = fun() ->
                case mnesia:read(sofia_sagas, SagaId) of
                    [R] ->
                        mnesia:write(R#sofia_sagas{completed_steps = [{Idx, Res} || {Idx, Res, _} <- NewCompleted]});
                    [] ->
                        ok
                end
            end,
            {atomic, _} = mnesia:transaction(F),
            execute_loop(SagaId, Rest, Index + 1, NewCompleted, TimeoutMs)
    end.

run_action_with_timeout(Action, TimeoutMs) ->
    Self = self(),
    {Pid, MRef} = spawn_monitor(fun() ->
        try run_action(Action) of
            Res -> Self ! {step_result, self(), Res}
        catch
            Class:Reason:Stacktrace ->
                Self ! {step_crashed, self(), Class, Reason, Stacktrace}
        end
    end),
    receive
        {step_result, Pid, Res} ->
            erlang:demonitor(MRef, [flush]),
            Res;
        {step_crashed, Pid, Class, Reason, Stacktrace} ->
            erlang:demonitor(MRef, [flush]),
            {crashed, Class, Reason, Stacktrace};
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, {process_down, Reason}}
    after TimeoutMs ->
        erlang:demonitor(MRef, [flush]),
        exit(Pid, kill),
        {error, step_timeout}
    end.


rollback_saga(SagaId, Completed) ->
    F = fun() ->
        case mnesia:read(sofia_sagas, SagaId) of
            [R] ->
                mnesia:write(R#sofia_sagas{status = rolling_back});
            [] ->
                ok
        end
    end,
    {atomic, _} = mnesia:transaction(F),
    
    CompensateResults = lists:map(
        fun({_Index, Result, Compensate}) ->
            try run_compensate(Compensate, Result) of
                CompRes -> {ok, CompRes}
            catch
                Class:Reason -> {error, {Class, Reason}}
            end
        end,
        Completed
    ),
    
    F2 = fun() ->
        case mnesia:read(sofia_sagas, SagaId) of
            [R] ->
                mnesia:write(R#sofia_sagas{status = rolled_back});
            [] ->
                ok
        end
    end,
    {atomic, _} = mnesia:transaction(F2),
    CompensateResults.

run_action(Fun) when is_function(Fun, 0) -> Fun();
run_action({M, F, A}) -> apply(M, F, A).

run_compensate(Fun, Result) when is_function(Fun, 1) -> Fun(Result);
run_compensate({M, F, A}, Result) -> apply(M, F, A ++ [Result]).

recover_sagas() ->
    F = fun() ->
        mnesia:select(sofia_sagas, [{#sofia_sagas{status = '$1', _ = '_'},
                                     [{'orelse', {'==', '$1', running}, {'==', '$1', rolling_back}}],
                                     ['$_']}])
    end,
    case mnesia:transaction(F) of
        {atomic, Sagas} ->
            [recover_saga(S) || S <- Sagas],
            ok;
        _ ->
            ok
    end.

recover_saga(#sofia_sagas{saga_id = SagaId, completed_steps = Completed, steps = Steps, retry_count = Retries}) ->
    if
        Retries >= ?MAX_RECOVERY_ATTEMPTS ->
            error_logger:error_msg("Saga ~p exceeded maximum recovery retries (~p), marking as failed.~n", [SagaId, ?MAX_RECOVERY_ATTEMPTS]),
            F = fun() ->
                case mnesia:read(sofia_sagas, SagaId) of
                    [R] -> mnesia:write(R#sofia_sagas{status = failed});
                    [] -> ok
                end
            end,
            mnesia:transaction(F),
            ok;
        true ->
            F = fun() ->
                case mnesia:read(sofia_sagas, SagaId) of
                    [R] -> mnesia:write(R#sofia_sagas{status = rolling_back, retry_count = Retries + 1});
                    [] -> ok
                end
            end,
            {atomic, _} = mnesia:transaction(F),
            
            lists:foreach(
                fun({Index, Result}) ->
                    case lists:nth(Index, Steps) of
                        {_Action, Compensate} ->
                            try run_compensate(Compensate, Result) of
                                _ -> ok
                            catch
                                Class:Reason ->
                                    error_logger:error_msg("Failed to run compensation during saga recovery: ~p:~p~n", [Class, Reason])
                            end;
                        _ ->
                            ok
                    end
                end,
                Completed
            ),
            
            F2 = fun() ->
                case mnesia:read(sofia_sagas, SagaId) of
                    [R] -> mnesia:write(R#sofia_sagas{status = rolled_back});
                    [] -> ok
                end
            end,
            {atomic, _} = mnesia:transaction(F2),
            ok
    end.


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Recover any crashed / running Sagas on startup
    spawn(fun() -> recover_sagas() end),
    {ok, #state{}}.

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
