%%%-------------------------------------------------------------------
%% @doc sofia top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(sofia_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = [
        #{
            id => sofia_registry,
            start => {sofia_registry, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_breaker,
            start => {sofia_breaker, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_config,
            start => {sofia_config, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_gateway,
            start => {sofia_gateway, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_router,
            start => {sofia_router, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_transformer,
            start => {sofia_transformer, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_saga,
            start => {sofia_saga, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_tracer,
            start => {sofia_tracer, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_rate_limiter,
            start => {sofia_rate_limiter, start_link, []},
            restart => permanent,
            type => worker
        },
        #{
            id => sofia_dlq,
            start => {sofia_dlq, start_link, []},
            restart => permanent,
            type => worker
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
