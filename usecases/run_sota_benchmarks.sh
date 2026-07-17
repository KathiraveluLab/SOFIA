#!/bin/bash
set -e

echo "========================================================="
# Locate redis-server
if command -v redis-server &> /dev/null; then
    REDIS_BIN="redis-server"
elif [ -f "./third_party/redis-7.2.4/src/redis-server" ]; then
    REDIS_BIN="./third_party/redis-7.2.4/src/redis-server"
else
    echo "Error: redis-server is not installed on the system and not found in third_party."
    echo "Please run compilation or install redis-server."
    exit 1
fi

# Locate consul
if command -v consul &> /dev/null; then
    CONSUL_BIN="consul"
elif [ -f "./third_party/consul" ]; then
    CONSUL_BIN="./third_party/consul"
else
    echo "Error: consul is not installed on the system and not found in third_party."
    echo "Please download consul or place it in third_party."
    exit 1
fi

echo "Starting SOTA services on bare hardware..."
# Start Redis
$REDIS_BIN --port 6379 --daemonize yes --pidfile /tmp/sofia-redis-bench.pid

# Start Consul
$CONSUL_BIN agent -dev -client 0.0.0.0 > /tmp/sofia-consul-bench.log 2>&1 &
CONSUL_PID=$!

# Wait for Consul HTTP port to be ready
echo "Waiting for services to initialize..."
for i in {1..15}; do
    if curl -s http://localhost:8500/v1/status/leader &> /dev/null; then
        echo "Consul is ready!"
        break
    fi
    sleep 1
done

# Run benchmarks
echo "Running Erlang benchmarks..."
rebar3 shell --eval "sofia_bench:run(), init:stop()."

# Clean up
echo "Cleaning up SOTA services..."
if [ -f "/tmp/sofia-redis-bench.pid" ]; then
    REDIS_PID=$(cat /tmp/sofia-redis-bench.pid)
    kill -9 $REDIS_PID 2>/dev/null || true
    rm -f /tmp/sofia-redis-bench.pid
else
    pkill -9 redis-server || true
fi

if [ ! -z "$CONSUL_PID" ]; then
    kill -9 $CONSUL_PID 2>/dev/null || true
else
    pkill -9 consul || true
fi

echo "Benchmarks completed successfully."
echo "========================================================="
