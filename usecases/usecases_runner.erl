-module(usecases_runner).
-export([run/0]).

run() ->
    %% Ensure SOFIA is started
    {ok, _} = application:ensure_all_started(sofia),

    io:format("=== Running Municipal Water Quality Monitoring Use Case ===~n"),
    ok = water_quality:run(),
    io:format("Water quality: OK~n~n"),

    io:format("=== Running Industrial Carbon Traceability Use Case ===~n"),
    ok = carbon_traceability:run(),
    io:format("Carbon traceability: OK~n~n"),

    io:format("=== Running Healthcare Outcome Verification Use Case ===~n"),
    ok = healthcare_outcome:run(),
    io:format("Healthcare outcome: OK~n~n"),

    io:format("=== Running Higher Education Credentials Use Case ===~n"),
    ok = education_credentials:run(),
    io:format("Education credentials: OK~n~n"),

    io:format("=== Running Healthcare and Finance Portal Interconnection Use Case ===~n"),
    ok = portal_interconnection:run(),
    io:format("Portal interconnection: OK~n~n"),

    io:format("All 5 use cases executed successfully!~n"),
    ok.
