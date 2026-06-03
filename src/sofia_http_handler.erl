-module(sofia_http_handler).
-behaviour(cowboy_handler).

-export([init/2]).

init(Req0, State) ->
    Method = cowboy_req:method(Req0),
    HasBody = cowboy_req:has_body(Req0),
    Req = handle_request(Method, HasBody, Req0),
    {ok, Req, State}.

handle_request(<<"POST">>, true, Req) ->
    ServiceNameBin = cowboy_req:binding(service_name, Req),
    ServiceAtom = list_to_atom(binary_to_list(ServiceNameBin)),
    
    case read_body_loop(Req, <<>>) of
        {ok, Body, Req2} ->
            try
                Parsed = jsx:decode(Body, [return_maps]),
                Normalized = normalize_keys(Parsed),
                %% 1. Check if the request is formatted for contract-based calls:
                %% e.g. {"method": "add", "payload": {"a": 5, "b": 10}}
                Result = case {maps:find(method, Normalized), maps:find(payload, Normalized)} of
                    {{ok, MethodStr}, {ok, PayloadMap}} when is_map(PayloadMap) andalso is_binary(MethodStr) ->
                        MethodAtom = list_to_atom(binary_to_list(MethodStr)),
                        sofia_client_stub:call_service(ServiceAtom, {MethodAtom, PayloadMap});
                    {{ok, MethodAtom}, {ok, PayloadMap}} when is_map(PayloadMap) andalso is_atom(MethodAtom) ->
                        sofia_client_stub:call_service(ServiceAtom, {MethodAtom, PayloadMap});
                    _ ->
                        %% Fallback: use legacy translation gateway
                        BreakerId = list_to_atom(atom_to_list(ServiceAtom) ++ "_breaker"),
                        sofia_gateway:handle_request(ServiceAtom, Normalized, BreakerId)
                end,
                
                case Result of
                    {ok, Reply} ->
                        send_response(200, #{status => <<"success">>, result => Reply}, Req2);
                    {error, {contract_validation_failed, ValError}} ->
                        send_response(400, #{
                            status => <<"error">>, 
                            reason => <<"contract_validation_failed">>, 
                            details => format_validation_error(ValError)
                        }, Req2);
                    {error, Reason} ->
                        send_response(500, #{
                            status => <<"error">>, 
                            reason => format_reason(Reason)
                        }, Req2)
                end
            catch
                _:Err ->
                    send_response(400, #{
                        status => <<"error">>, 
                        reason => <<"invalid_json">>, 
                        details => list_to_binary(io_lib:format("~p", [Err]))
                    }, Req2)
            end;
        {error, Reason, Req2} ->
            send_response(500, #{
                status => <<"error">>, 
                reason => <<"failed_to_read_body">>, 
                details => format_reason(Reason)
            }, Req2)
    end;
handle_request(_, _, Req) ->
    send_response(405, #{status => <<"error">>, reason => <<"method_not_allowed">>}, Req).

read_body_loop(Req0, Acc) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req} ->
            {ok, <<Acc/binary, Data/binary>>, Req};
        {more, Data, Req} ->
            read_body_loop(Req, <<Acc/binary, Data/binary>>)
    end.

send_response(StatusCode, MapPayload, Req) ->
    Json = jsx:encode(MapPayload),
    cowboy_req:reply(StatusCode, #{
        <<"content-type">> => <<"application/json">>
    }, Json, Req).

format_validation_error({missing_parameter, Key}) ->
    list_to_binary(io_lib:format("Missing parameter: ~p", [Key]));
format_validation_error({type_mismatch, Key, Expected, Value}) ->
    list_to_binary(io_lib:format("Type mismatch for ~p. Expected ~p, got ~p", [Key, Expected, Value]));
format_validation_error(Other) ->
    list_to_binary(io_lib:format("~p", [Other])).

format_reason(Reason) when is_atom(Reason) -> atom_to_binary(Reason);
format_reason(Reason) -> list_to_binary(io_lib:format("~p", [Reason])).

normalize_keys(Map) when is_map(Map) ->
    maps:fold(fun(Key, Val, Acc) ->
        NewKey = if is_binary(Key) ->
                        try binary_to_existing_atom(Key, utf8)
                        catch _:_ -> binary_to_atom(Key, utf8)
                        end;
                    true -> Key
                 end,
        NewVal = if is_map(Val) -> normalize_keys(Val);
                    true -> Val
                 end,
        maps:put(NewKey, NewVal, Acc)
    end, #{}, Map);
normalize_keys(Other) -> Other.
