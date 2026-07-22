#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "========================================================="
echo "  SOFIA Master Reproducibility & Verification Suite      "
echo "========================================================="

# 1. Build and Run EUnit Unit Tests
echo "[1/4] Running EUnit test suite..."
cd "$ROOT_DIR"
rebar3 eunit

# 2. Run Single-Node Micro-Benchmarks (sofia_bench)
echo "[2/4] Running subsystem micro-benchmarks (sofia_bench)..."
rebar3 shell --eval "sofia_bench:run(), init:stop()."

# 3. Run Clustered Distributed Benchmarks
echo "[3/4] Running 2-node clustered deployment benchmark..."
cd "$SCRIPT_DIR"
chmod +x run_distributed_deployment.sh
./run_distributed_deployment.sh

# 4. Compile Paper LaTeX Manuscript
echo "[4/4] Compiling IEEE TSC manuscript PDF..."
cd "$ROOT_DIR/_paper"
pdflatex -interaction=nonstopmode main.tex > /dev/null || true
bibtex main > /dev/null || true
pdflatex -interaction=nonstopmode main.tex > /dev/null || true
pdflatex -interaction=nonstopmode main.tex > /dev/null || true

echo "========================================================="
echo "  Reproducibility suite completed successfully!         "
echo "  Manuscript available at: _paper/main.pdf               "
echo "========================================================="
