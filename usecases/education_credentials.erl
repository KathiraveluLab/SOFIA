-module(education_credentials).
-export([run/0, start_verifier/1, verifier_loop/1]).

start_verifier(Domain) ->
    spawn(fun() -> verifier_loop(Domain) end).

verifier_loop(Domain) ->
    receive
        {'$gen_call', From, {verify_credential, StudentId, DiplomaHash}} ->
            gen_server:reply(From, {ok, {verified, Domain, StudentId, DiplomaHash}});
        stop ->
            ok;
        _ ->
            verifier_loop(Domain)
    end,
    verifier_loop(Domain).

run() ->
    Pid1 = start_verifier(<<"mit.edu">>),
    Pid2 = start_verifier(<<"harvard.edu">>),

    %% 1. Register verifiers with different metadata (region, api_version)
    ok = sofia_registry:register_service(credential_verifier, Pid1, #{}, #{<<"region">> => <<"us-east">>, <<"version">> => <<"2.1">>}),
    ok = sofia_registry:register_service(credential_verifier, Pid2, #{}, #{<<"region">> => <<"us-west">>, <<"version">> => <<"2.2">>}),

    %% 2. Discover all and perform load-balancing
    {ok, Pids} = case sofia_registry:discover_all(credential_verifier) of
        [] -> {error, empty};
        List -> {ok, List}
    end,
    2 = length(Pids),

    %% Discover with random load-balancing
    {ok, SelectedPid} = sofia_registry:discover(credential_verifier),
    true = lists:member(SelectedPid, [Pid1, Pid2]),

    %% 3. Context-sensitive queries via discover_by_metadata/2
    {ok, MitPid} = sofia_registry:discover_by_metadata(credential_verifier, #{<<"region">> => <<"us-east">>}),
    Pid1 = MitPid,

    {ok, HarvardPid} = sofia_registry:discover_by_metadata(credential_verifier, #{<<"region">> => <<"us-west">>}),
    Pid2 = HarvardPid,

    %% 4. Self-healing property: kill Pid1 and verify it is evicted automatically
    exit(Pid1, kill),
    timer:sleep(50),

    %% Pid1 should be gone from registry
    PidsAfterKill = sofia_registry:discover_all(credential_verifier),
    false = lists:member(Pid1, PidsAfterKill),
    true = lists:member(Pid2, PidsAfterKill),

    {error, no_service_available} = sofia_registry:discover_by_metadata(credential_verifier, #{<<"region">> => <<"us-east">>}),

    %% Clean up
    ok = sofia_registry:deregister_service(credential_verifier, Pid2),
    Pid2 ! stop,
    ok.
