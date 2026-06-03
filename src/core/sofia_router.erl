-module(sofia_router).
-behaviour(gen_server).

%% API
-export([start_link/0, route/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Dynamically route payload to a specific service instance using a custom routing/criteria function
route(ServiceType, Payload, RoutingKeyFun) ->
    case sofia_registry:discover_all(ServiceType) of
        [] ->
            {error, no_service_available};
        Pids ->
            MaxQueueLen = application:get_env(sofia, max_mailbox_size, 100),
            HealthyPids = lists:filter(fun(Pid) ->
                case erlang:process_info(Pid, message_queue_len) of
                    {message_queue_len, Len} when Len > MaxQueueLen -> false;
                    undefined -> false;
                    _ -> true
                end
            end, Pids),
            case HealthyPids of
                [] ->
                    {error, overloaded};
                _ ->
                    %% Apply the dynamic routing criteria to choose a specific Pid
                    case RoutingKeyFun(Payload, HealthyPids) of
                        {ok, SelectedPid} ->
                            {ok, SelectedPid};
                        {error, Reason} ->
                            {error, {routing_failed, Reason}}
                    end
            end
    end.

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
