#!/bin/bash
set -e

echo "========================================================="
# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: docker is not installed. Please install Docker to run SOTA benchmarks."
    exit 1
fi

echo "Spinning up SOTA benchmark containers..."
# Spin up Redis
docker run -d --name sofia-redis-bench -p 6379:6379 redis:alpine
# Spin up Consul
docker run -d --name sofia-consul-bench -p 8500:8500 hashicorp/consul:latest agent -dev -client 0.0.0.0

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
echo "Cleaning up containers..."
docker rm -f sofia-redis-bench sofia-consul-bench
echo "Benchmarks completed successfully."
echo "========================================================="
