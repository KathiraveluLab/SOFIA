%%%-------------------------------------------------------------------
%% @doc sofia public API
%% @end
%%%-------------------------------------------------------------------

-module(sofia_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    sofia_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
