-module(sofia_contract).
-export([validate_request/3]).

%% @doc Validates a client request payload against a registered service contract.
validate_request(Contract, Method, Payload) ->
    case maps:find(methods, Contract) of
        {ok, Methods} ->
            case maps:find(Method, Methods) of
                {ok, MethodSpec} ->
                    case maps:find(input_schema, MethodSpec) of
                        {ok, InputSchema} ->
                            validate_schema(InputSchema, Payload);
                        error ->
                            ok
                    end;
                error ->
                    {error, {unknown_method, Method}}
            end;
        error ->
            {error, invalid_contract}
    end.

validate_schema(Schema, Payload) ->
    maps:fold(fun(Key, ExpectedType, Acc) ->
        case Acc of
            ok ->
                case find_payload_value(Key, Payload) of
                    {ok, Value} ->
                        case check_type(Value, ExpectedType) of
                            true -> ok;
                            false -> {error, {type_mismatch, Key, ExpectedType, Value}}
                        end;
                    error ->
                        {error, {missing_parameter, Key}}
                end;
            {error, _} = Err ->
                Err
        end
    end, ok, Schema).

find_payload_value(Key, Payload) when is_atom(Key) ->
    case maps:find(Key, Payload) of
        {ok, V} -> {ok, V};
        error ->
            BinKey = atom_to_binary(Key, utf8),
            maps:find(BinKey, Payload)
    end;
find_payload_value(Key, Payload) ->
    maps:find(Key, Payload).

check_type(Value, integer) -> is_integer(Value);
check_type(Value, float) -> is_float(Value);
check_type(Value, number) -> is_number(Value);
check_type(Value, binary) -> is_binary(Value);
check_type(Value, list) -> is_list(Value);
check_type(Value, map) -> is_map(Value);
check_type(Value, boolean) -> is_boolean(Value);
check_type(Value, string) ->
    is_list(Value) andalso lists:all(fun(X) -> is_integer(X) andalso X >= 0 andalso X =< 1114111 end, Value);
check_type(_, _) -> false.
