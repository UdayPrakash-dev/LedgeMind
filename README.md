# LedgeMind

An event-sourced wallet ledger whose stuck transfers (sagas) are repaired by an LLM that picks from a fixed action menu, with a deterministic verifier approving every fix before it runs.

## Layout

```
docker-compose.yml     runs Postgres, Redis, wallet-service, recovery-service
wallet-service/        Go: event store, wallets, snapshots, transfer sagas
  migrations/          SQL tables, applied automatically on every start
  eventstore/          append-only event store with optimistic concurrency
  domain/              wallet rules (pure, no I/O) + snapshots
  ledger/              load -> decide -> append loop, Redis snapshot cache
  saga/                transfer saga: debit -> payment rail -> credit, fixed compensations
  api/                 HTTP endpoints
recovery-service/      Python: AI planner, verifier, resolution memory (Day 6+)
fake-rail/             fake payment service with fault injection (Day 4)
bench/                 benchmark harness (Day 8)
paper/                 LaTeX source
```

## Run locally (Windows cmd)

Prerequisites: Docker Desktop, Go 1.24+, Git.

```cmd
copy .env.example .env
cd wallet-service && go mod tidy && cd ..
docker compose up --build -d
docker compose ps
curl http://localhost:8080/health
```

## Run the tests

Needs Postgres and Redis from the step above.

```cmd
cd wallet-service
set "DATABASE_URL=postgres://ledgemind:ledgemind@localhost:5432/ledgemind?sslmode=disable"
set "REDIS_ADDR=localhost:6379"
go test ./...
```

Snapshot load benchmark: `go test -run ^$ -bench Load ./ledger`

## API

All money is in paise (int64). Writes need an `Idempotency-Key` header.

| Request | Body | Reply |
| --- | --- | --- |
| `POST /wallets` | `{"owner":"uday"}` | 201 wallet |
| `GET /wallets/{id}` | | 200 wallet / 404 |
| `POST /wallets/{id}/credit` | `{"amount_paise":50000}` | 200 (`applied:false` = repeat); 400 / 409 |
| `POST /wallets/{id}/debit` | `{"amount_paise":20000}` | 200; 422 insufficient funds |
| `POST /transfers` | `{"from_wallet":"..","to_wallet":"..","amount_paise":1000}` | 201 new / 200 repeat; outcome in `status` |
| `GET /transfers/{id}` | | 200 transfer with step history / 404 |

Transfer statuses: `COMPLETED`, `FAILED` (debit rejected, nothing moved), `COMPENSATED` (sender refunded), `STUCK` (refund failed; needs recovery), `RUNNING`.

## Useful commands

| Command | What it does |
| --- | --- |
| `docker compose logs -f wallet-service` | follow one service's logs |
| `docker compose down` | stop everything (data kept) |
| `docker compose down -v` | stop and wipe Postgres and Redis data |
| `docker exec -it ledgemind-postgres psql -U ledgemind` | open a SQL shell |
| `docker exec -it ledgemind-redis redis-cli keys "snap:*"` | list wallet snapshots |
