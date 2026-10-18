#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "========================================================="
echo "  SOFIA Master Reproducibility & Verification Suite      "
echo "========================================================="

# 1. Build and Run EUnit Unit Tests
echo "[1/3] Running EUnit test suite..."
cd "$ROOT_DIR"
rebar3 eunit

# 2. Run Single-Node Micro-Benchmarks & Use Case Pipeline
echo "[2/3] Running subsystem micro-benchmarks and use case pipeline..."
rebar3 shell --eval "sofia_bench:run(), usecases_runner:run(), benchmark_usecases:run(), init:stop()."

# 3. Run Clustered Distributed Benchmarks
echo "[3/3] Running 2-node clustered deployment benchmark..."
cd "$SCRIPT_DIR"
chmod +x run_distributed_deployment.sh
./run_distributed_deployment.sh

echo "========================================================="
echo "  Reproducibility suite completed successfully!         "
echo "========================================================="
