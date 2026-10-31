# LedgeMind (Linux / macOS / the deployment VM). On Windows use bench.bat instead.
SEED ?= 1

.PHONY: up smoke bench bench-all-seeds ablations aggregate test

up:
	docker compose up -d --build

smoke:            ## 10 failures (2 per fault), short throughput test
	docker compose --profile bench run --rm bench --n 10 --seed $(SEED) --seconds 5

bench:            ## the full experiment: 100 failures x 4 strategies + throughput
	docker compose --profile bench run --rm bench --n 100 --seed $(SEED)

bench-all-seeds:  ## the 3 seeds for the paper
	for s in 1 2 3; do docker compose --profile bench run --rm bench --n 100 --seed $$s || exit 1; done

ablations:       ## B (rail hidden) and C (10/20/30% injected errors) on SEED
	docker compose --profile bench run --rm bench --n 100 --seed $(SEED) --hide-rail
	for r in 0.1 0.2 0.3; do docker compose --profile bench run --rm bench --n 100 --seed $(SEED) --inject-rate $$r || exit 1; done

aggregate:       ## paper tables + figures -> bench/results/paper
	docker run --rm -v "$(CURDIR)/bench:/bench" -w /bench python:3.12-slim sh -c "pip install -q matplotlib && python aggregate.py"

test:
	cd fake-rail && go test ./...
	cd wallet-service && go test ./...
