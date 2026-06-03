-module(sofia_workflow).
-export([parse_yaml/1, execute/4, forward_step/5, complete_step/5]).

%% @doc Parses a YAML directed hypergraph representation of a service workflow.
parse_yaml(YamlStr) ->
    Lines = string:lexemes(YamlStr, "\r\n"),
    parse_lines(Lines, undefined, undefined, []).

parse_lines([], Name, _CurrentSource, Edges) ->
    {ok, #{name => Name, edges => maps:from_list(Edges)}};
parse_lines([Line | Rest], Name, CurrentSource, Edges) ->
    Trimmed = string:trim(Line),
    case Trimmed of
        "" ->
            parse_lines(Rest, Name, CurrentSource, Edges);
        "#" ++ _Comment ->
            parse_lines(Rest, Name, CurrentSource, Edges);
        "name: " ++ WfName ->
            parse_lines(Rest, string:trim(WfName), CurrentSource, Edges);
        "- source: " ++ Source ->
            parse_lines(Rest, Name, list_to_atom(string:trim(Source)), Edges);
        "destinations:" ->
            {Dests, RemainingLines} = parse_destinations(Rest, []),
            NewEdges = case CurrentSource of
                undefined -> Edges;
                Src -> [{Src, Dests} | Edges]
            end,
            parse_lines(RemainingLines, Name, CurrentSource, NewEdges);
        _ ->
            parse_lines(Rest, Name, CurrentSource, Edges)
    end.

parse_destinations([], Acc) ->
    {lists:reverse(Acc), []};
parse_destinations([Line | Rest] = All, Acc) ->
    Trimmed = string:trim(Line),
    case Trimmed of
        "- source: " ++ _ ->
            {lists:reverse(Acc), All};
        "- " ++ Dest ->
            parse_destinations(Rest, [list_to_atom(string:trim(Dest)) | Acc]);
        "" ->
            parse_destinations(Rest, Acc);
        _ ->
            {lists:reverse(Acc), All}
    end.

%% @doc Triggers the execution of a workflow at a specific start node.
execute(Workflow, StartNode, Payload, Originator) ->
    forward_step(Workflow, StartNode, Payload, make_ref(), Originator).

%% @doc Executes a single step of the workflow by routing it to the registered node.
forward_step(#{edges := _Edges} = Workflow, Node, Payload, Ref, Originator) ->
    case sofia_registry:discover(Node) of
        {ok, Pid} ->
            Pid ! {workflow_step, Workflow, Node, Payload, Ref, Originator},
            ok;
        {error, Reason} ->
            Originator ! {workflow_error, Node, Reason},
            {error, {discovery_failed, Node, Reason}}
    end.

%% @doc Called by a step service when it completes execution.
%% Routes the output payload to all destinations of the current source node.
complete_step(#{edges := Edges} = Workflow, Node, NewPayload, Ref, Originator) ->
    case maps:get(Node, Edges, []) of
        [] ->
            %% Leaf node: report branch completion to the originator
            Originator ! {workflow_branch_complete, Node, NewPayload, Ref},
            ok;
        Dests ->
            %% Fan-out (hyperedge routing) to all destination nodes
            lists:foreach(fun(Dest) ->
                forward_step(Workflow, Dest, NewPayload, Ref, Originator)
            end, Dests),
            ok
    end.
