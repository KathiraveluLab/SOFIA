-module(sofia_auth).
-export([sign_payload/3, verify_payload/4, get_client_secret/1, set_client_secret/2]).

-define(TABLE, sofia_auth_secrets).
-define(REPLAY_WINDOW_SECS, 300). %% 5 minutes

%% @doc Computes the HMAC-SHA256 signature for a payload.
sign_payload(ClientId, Timestamp, PayloadBin) when is_binary(ClientId), is_binary(Timestamp), is_binary(PayloadBin) ->
    case get_client_secret(ClientId) of
        {ok, Secret} ->
            DataToSign = <<ClientId/binary, ".", Timestamp/binary, ".", PayloadBin/binary>>,
            Signature = crypto:mac(hmac, sha256, Secret, DataToSign),
            {ok, binary_to_hex(Signature)};
        error ->
            {error, client_not_found}
    end.

%% @doc Verifies the signature of a payload.
verify_payload(ClientId, Timestamp, SignatureHex, PayloadBin) when is_binary(ClientId), is_binary(Timestamp), is_binary(SignatureHex), is_binary(PayloadBin) ->
    case verify_timestamp(Timestamp) of
        false ->
            {error, replay_or_skewed_clock};
        true ->
            case get_client_secret(ClientId) of
                {ok, Secret} ->
                    DataToSign = <<ClientId/binary, ".", Timestamp/binary, ".", PayloadBin/binary>>,
                    ExpectedSig = crypto:mac(hmac, sha256, Secret, DataToSign),
                    ExpectedHex = binary_to_hex(ExpectedSig),
                    case SignatureHex =:= ExpectedHex of
                        true -> ok;
                        false -> {error, invalid_signature}
                    end;
                error ->
                    {error, client_not_found}
            end
    end.

%% @doc Helper to convert binary signatures to hex string.
binary_to_hex(Binary) ->
    << <<(hex_digit(N)):8>> || <<N:4>> <= Binary >>.

hex_digit(N) when N >= 0, N =< 9 -> N + $0;
hex_digit(N) when N >= 10, N =< 15 -> N - 10 + $a.

%% @doc Check if the timestamp (seconds since epoch) is within the replay window.
verify_timestamp(TimestampBin) ->
    try
        Timestamp = binary_to_integer(TimestampBin),
        Now = erlang:system_time(second),
        abs(Now - Timestamp) =< ?REPLAY_WINDOW_SECS
    catch
        _:_ -> false
    end.

%% @doc Mnesia-based storage of client secrets.
get_client_secret(ClientId) ->
    F = fun() -> mnesia:read(sofia_client_secrets, ClientId) end,
    case mnesia:transaction(F) of
        {atomic, [{sofia_client_secrets, ClientId, Secret}]} -> {ok, Secret};
        _ -> error
    end.

set_client_secret(ClientId, Secret) when is_binary(ClientId), is_binary(Secret) ->
    F = fun() -> mnesia:write({sofia_client_secrets, ClientId, Secret}) end,
    case mnesia:transaction(F) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.
