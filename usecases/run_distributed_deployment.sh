#!/usr/bin/env bash
set -e

# Make sure compilation is up to date
rebar3 compile

# Create separate database directories for each node to prevent clashes
rm -rf _build/node1_db _build/node2_db
mkdir -p _build/node1_db _build/node2_db

# Get local hostname
HOSTNAME=$(hostname)

echo "Starting SOFIA Node 1..."
erl -sname sofia_node1 -setcookie sofia_secret -mnesia dir '"_build/node1_db"' -noshell -pa _build/default/lib/*/ebin -eval "application:ensure_all_started(sofia)." &
NODE1_PID=$!

echo "Starting SOFIA Node 2..."
# Change HTTP gateway port for node 2 to prevent TCP address collision (node 1 uses 8080, node 2 uses 8081)
erl -sname sofia_node2 -setcookie sofia_secret -mnesia dir '"_build/node2_db"' -noshell -pa _build/default/lib/*/ebin -eval "application:set_env(sofia, gateway_port, 8081), application:ensure_all_started(sofia)." &
NODE2_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "Shutting down deployed nodes..."
    erl -sname sofia_cleanup -setcookie sofia_secret -noshell -eval "
        rpc:cast(list_to_atom(\"sofia_node1@\" ++ net_adm:localhost()), init, stop, []),
        rpc:cast(list_to_atom(\"sofia_node2@\" ++ net_adm:localhost()), init, stop, []),
        init:stop().
    "
    # Wait for background processes to terminate
    wait $NODE1_PID 2>/dev/null || true
    wait $NODE2_PID 2>/dev/null || true
    rm -rf _build/node1_db _build/node2_db
    echo "Cleanup complete."
}
trap cleanup EXIT

# Wait a short duration to ensure nodes boot up fully
sleep 3

echo "Running Distributed Deployment Test..."
erl -sname sofia_orchestrator -setcookie sofia_secret -noshell -pa _build/default/lib/*/ebin -eval "
    case distributed_test:run() of
        ok -> init:stop(0);
        _ -> init:stop(1)
    end.
"
