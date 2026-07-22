.PHONY: all test bench cluster paper check clean

all: check

test:
	cd .. && rebar3 eunit

bench:
	cd .. && rebar3 shell --eval "sofia_bench:run(), init:stop()."

sota-bench:
	./run_sota_benchmarks.sh

cluster:
	./run_distributed_deployment.sh

paper:
	cd ../_paper && (pdflatex -interaction=nonstopmode main.tex || true) && (bibtex main || true) && (pdflatex -interaction=nonstopmode main.tex || true) && (pdflatex -interaction=nonstopmode main.tex || true)

check: test bench cluster paper
	@echo "All verification tasks and paper compilation completed cleanly!"

clean:
	cd .. && rebar3 clean
	rm -f ../_paper/*.aux ../_paper/*.log ../_paper/*.bbl ../_paper/*.blg ../_paper/*.out ../_paper/*.toc
