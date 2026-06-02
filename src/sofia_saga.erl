-module(sofia_saga).
-behaviour(gen_server).

%% API
-export([start_link/0, execute/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Executes a list of steps in a Saga orchestration.
%% Each step is of form: {ActionFun, CompensateFun}
%% If any step returns {error, Reason} or throws, we halt and execute CompensateFuns in reverse order.
execute(Steps) ->
    execute(Steps, []).

execute([], Completed) ->
    Results = [Res || {Res, _Compensate} <- lists:reverse(Completed)],
    {ok, Results};
execute([{Action, Compensate} | Rest], Completed) ->
    try Action() of
        {ok, Result} ->
            execute(Rest, [{Result, Compensate} | Completed]);
        {error, Reason} ->
            CompensateResults = rollback(Completed),
            {error, {step_failed, Reason, CompensateResults}};
        Other ->
            execute(Rest, [{Other, Compensate} | Completed])
    catch
        Class:Reason:Stacktrace ->
            CompensateResults = rollback(Completed),
            {error, {step_crashed, Class, Reason, Stacktrace, CompensateResults}}
    end.

rollback(Completed) ->
    lists:map(
        fun({Result, Compensate}) ->
            try Compensate(Result) of
                CompRes -> {ok, CompRes}
            catch
                Class:Reason -> {error, {Class, Reason}}
            end
        end,
        Completed
    ).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
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
