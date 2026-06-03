-module(sofia_pipeline).
-export([register_worker/1, deregister_worker/1, push_task/2]).

%% @doc Registers the calling process as a pipeline worker.
register_worker(PipelineId) ->
    sofia_registry:register_service({pipeline, PipelineId}, self()).

%% @doc Deregisters the calling process from the pipeline.
deregister_worker(PipelineId) ->
    sofia_registry:deregister_service({pipeline, PipelineId}, self()).

%% @doc Pushes a task to an active worker in the pipeline via load balancing.
push_task(PipelineId, Task) ->
    case sofia_registry:discover({pipeline, PipelineId}) of
        {ok, WorkerPid} ->
            WorkerPid ! {sofia_pipeline_task, PipelineId, Task},
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
