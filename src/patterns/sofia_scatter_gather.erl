-module(sofia_scatter_gather).
-export([request/3]).

%% @doc Sends a request to all discovered processes of ServiceType,
%% gathers their replies, and returns the aggregated list of responses.
request(ServiceType, RequestMsg, Timeout) ->
    case sofia_registry:discover_all(ServiceType) of
        [] ->
            {error, no_service_available};
        Pids ->
            Ref = make_ref(),
            Self = self(),
            %% Scatter: Send message to all members asynchronously
            lists:foreach(fun(Pid) ->
                Pid ! {scatter, Ref, Self, RequestMsg}
            end, Pids),
            %% Gather: Collect responses
            Replies = gather(Ref, length(Pids), Timeout, []),
            {ok, Replies}
    end.

%% Internal gather loop
gather(_Ref, 0, _Timeout, Acc) ->
    Acc;
gather(Ref, N, Timeout, Acc) ->
    Start = erlang:monotonic_time(millisecond),
    receive
        {gather, Ref, Reply} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            Remaining = Timeout - Elapsed,
            case Remaining > 0 of
                true -> gather(Ref, N - 1, Remaining, [Reply | Acc]);
                false -> [Reply | Acc]
            end
    after Timeout ->
        Acc
    end.
