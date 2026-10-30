# LedgeMind (Linux / macOS / the deployment VM). On Windows use bench.bat instead.
SEED ?= 1

.PHONY: up smoke bench bench-all-seeds test

up:
	docker compose up -d --build

smoke:            ## 10 failures (2 per fault), short throughput test
	docker compose --profile bench run --rm bench --n 10 --seed $(SEED) --seconds 5

bench:            ## the full experiment: 100 failures x 4 strategies + throughput
	docker compose --profile bench run --rm bench --n 100 --seed $(SEED)

bench-all-seeds:  ## the 3 seeds for the paper
	for s in 1 2 3; do docker compose --profile bench run --rm bench --n 100 --seed $$s || exit 1; done

test:
	cd fake-rail && go test ./...
	cd wallet-service && go test ./...
