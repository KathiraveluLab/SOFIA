-module(sofia_transformer).
-behaviour(gen_server).

%% API
-export([start_link/0, transform/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Transforms payload keys and values using a Map containing transform rules or a mapping function
transform(Payload, RulesMap) when is_map(Payload), is_map(RulesMap) ->
    maps:fold(
        fun(SrcKey, DestKey, Acc) ->
            case maps:find(SrcKey, Payload) of
                {ok, Value} ->
                    Acc1 = maps:remove(SrcKey, Acc),
                    maps:put(DestKey, Value, Acc1);
                error ->
                    Acc
            end
        end,
        Payload,
        RulesMap
    );
transform(Payload, TransformFun) when is_function(TransformFun, 1) ->
    TransformFun(Payload).

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
