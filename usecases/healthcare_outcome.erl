-module(healthcare_outcome).
-export([run/0]).

run() ->
    %% Let's create an ETS table to track simulated clinic state
    Table = ets:new(healthcare_saga_state, [public, set]),
    ets:insert(Table, {bed_reserved, false}),
    ets:insert(Table, {insurance_cleared, false}),

    %% 1. Define Success Saga Steps
    ReserveBedSuccess = fun() ->
        ets:insert(Table, {bed_reserved, true}),
        {ok, bed_id_104}
    end,
    ReleaseBed = fun(_BedId) ->
        ets:insert(Table, {bed_reserved, false}),
        ok
    end,
    VerifyInsuranceSuccess = fun() ->
        ets:insert(Table, {insurance_cleared, true}),
        {ok, insurance_ref_551}
    end,
    CancelInsurance = fun(_InsId) ->
        ets:insert(Table, {insurance_cleared, false}),
        ok
    end,

    SuccessSteps = [
        {ReserveBedSuccess, ReleaseBed},
        {VerifyInsuranceSuccess, CancelInsurance}
    ],

    %% Execute success Saga
    {ok, [bed_id_104, insurance_ref_551]} = sofia_saga:execute(SuccessSteps),
    [{bed_reserved, true}] = ets:lookup(Table, bed_reserved),
    [{insurance_cleared, true}] = ets:lookup(Table, insurance_cleared),

    %% Reset ETS table state
    ets:insert(Table, {bed_reserved, false}),
    ets:insert(Table, {insurance_cleared, false}),

    %% 2. Define Failure Saga Steps (insurance verification fails)
    VerifyInsuranceFailure = fun() ->
        {error, insufficient_coverage}
    end,

    FailureSteps = [
        {ReserveBedSuccess, ReleaseBed},
        {VerifyInsuranceFailure, CancelInsurance}
    ],

    %% Execute failure Saga
    {error, {step_failed, insufficient_coverage, _}} = sofia_saga:execute(FailureSteps),

    %% Verify that compensation rolled back the bed reservation
    [{bed_reserved, false}] = ets:lookup(Table, bed_reserved),
    [{insurance_cleared, false}] = ets:lookup(Table, insurance_cleared),

    %% Clean up
    ets:delete(Table),
    ok.
