.PHONY: all test bench usecases sota-bench cluster check clean

all: check

test:
	cd .. && rebar3 eunit

bench:
	cd .. && rebar3 shell --eval "sofia_bench:run(), init:stop()."

usecases:
	cd .. && rebar3 shell --eval "usecases_runner:run(), benchmark_usecases:run(), init:stop()."

sota-bench:
	./run_sota_benchmarks.sh

cluster:
	./run_distributed_deployment.sh

check: test bench usecases cluster
	@echo "All test suites, benchmarks, and clustered deployment verification passed cleanly!"

clean:
	cd .. && rebar3 clean
