%%%-------------------------------------------------------------------
%% @doc sofia public API
%% @end
%%%-------------------------------------------------------------------

-module(sofia_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    init_mnesia(),
    sofia_sup:start_link().

stop(_State) ->
    ok.

init_mnesia() ->
    case mnesia:system_info(is_running) of
        yes -> ok;
        _ ->
            mnesia:start(),
            case mnesia:create_schema([node()]) of
                ok -> ok;
                {error, {_, {already_exists, _}}} -> ok;
                _ -> ok
            end
    end,
    Tables = [
        {sofia_client_secrets, [client_id, secret]},
        {span, [span_id, trace_id, parent_span_id, name, start_time, end_time, duration]},
        {sofia_sagas, [saga_id, status, completed_steps, total_steps, steps]}
    ],
    lists:foreach(fun({Name, Attrs}) ->
        case mnesia:create_table(Name, [{disc_copies, [node()]}, {attributes, Attrs}, {type, set}]) of
            {atomic, ok} -> ok;
            {aborted, {already_exists, Name}} -> ok;
            {aborted, {bad_type, _, _, _}} ->
                %% Fallback to ram_copies if schema is ram-only (e.g. in test env)
                mnesia:create_table(Name, [{ram_copies, [node()]}, {attributes, Attrs}, {type, set}]);
            _Other -> ok
        end
    end, Tables),
    mnesia:wait_for_tables([sofia_client_secrets, span, sofia_sagas], 5000).
