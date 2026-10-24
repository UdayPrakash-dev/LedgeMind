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
  saga/                transfer saga, recovery policies, executor (the only way to fix a stuck transfer)
  audit/               invariant checks: money conserved, ledger matches the rail
  stuckqueue/          pushes STUCK transfers to the Redis Stream "stuck-sagas"
  api/                 HTTP endpoints
recovery-service/      Python: AI planner, verifier, resolution memory (Day 6+)
fake-rail/             Go: fake payment rail with 5 repeatable fault modes
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
cd fake-rail && go test ./... && cd ..
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

Transfer statuses: `COMPLETED`, `FAILED` (debit rejected, nothing moved), `COMPENSATED` (sender refunded), `STUCK` (waiting for recovery), `ESCALATED` (handed to a human), `RUNNING`.

## Recovery

**Policy** (what a new transfer does when the rail fails), set with `POST /admin/policy {"policy":"..."}` or `RECOVERY_POLICY`:

| Policy | Rail fails | Credit keeps failing |
| --- | --- | --- |
| `refund` (baseline 1) | refund sender | refund sender |
| `retry` (baseline 2) | retry after 1s, 2s, 4s, then STUCK | STUCK |
| `escalate` (AI strategies) | STUCK at once | STUCK |

Every STUCK transfer is added to the Redis Stream `stuck-sagas` (fields: `saga_id, step, policy, error, history, amount_paise, at`).

**Executor**: `POST /transfers/{id}/actions`, only for STUCK transfers:

| Action | Body | Effect |
| --- | --- | --- |
| `retry_step` | `{"action":"retry_step"}` | run the failed step again |
| `compensate` | `{"action":"compensate"}` | take back any receiver credit, refund the sender |
| `reroute` | `{"action":"reroute","rail":"backup"}` | pay again under a new rail payment id (only at CALL_RAIL) |
| `partial_refund` | `{"action":"partial_refund","amount_paise":400}` | refund part of the amount |
| `escalate` | `{"action":"escalate","reason":"..."}` | hand to a human |

Add `"actor"` and `"reason"` to any action; both are saved in the transfer's history. The executor keeps the ledger safe (no negative balances, nothing applied twice) but does not judge whether an action is wise; that is the verifier's job.

**Tools for a recovery strategy**: `GET /transfers/{id}/money` (what happened in the ledger), `GET /transfers/{id}/rail` (what the rail says), `GET /transfers?status=STUCK`.

**Checks**: `GET /admin/invariants` (add `?saga_id=` or `?since=2026-10-24T10:00:00Z`; `&rail=false` skips rail lookups). `POST /admin/reconcile` marks completed transfers whose rail payment was later reversed as STUCK at step `RECONCILE`.

| Violation | Meaning |
| --- | --- |
| `rail_paid_but_refunded` | sender refunded although the rail paid: money left twice |
| `delivered_but_rail_not_paid` | receiver credited although the rail rejected or reversed |
| `double_rail_payment` | the rail paid more than once for one transfer |
| `money_created` / `over_delivery` | a transfer gave back or delivered more than it took |
| `unsettled_terminal_saga` | a finished transfer still has money in flight |
| `negative_balance`, `snapshot_mismatch`, `conservation` | wallet-level problems |

## Fake payment rail (port 9000)

| Mode | Rail replies | What really happened | Correct recovery |
| --- | --- | --- | --- |
| `none` | 200 PAID | PAID | continue |
| `reject` | 422 limit exceeded | REJECTED | refund sender |
| `timeout` | nothing (wallet gives up after 2s) | PAID | continue, do NOT refund |
| `duplicate` | 409 duplicate | PAID | continue, do NOT refund |
| `partial` | 200 PAID | PAID, REVERSED 3s later | refund sender |
| `unknown` | 500 strange error | PAID or REJECTED (hidden) | check status, then decide |

Repeating a payment for the same `saga_id` never charges twice; it returns the stored outcome.

How the fault for each payment is chosen (first match wins):
1. `X-Fault-Mode` header on `POST /pay` (for testing the rail directly)
2. Queue: `POST /admin/faults {"modes":["timeout","reject"]}`, used in order
3. Random: `fault_rate` + `weights`, from a seeded generator (same seed and call order = same faults)

| Endpoint | Purpose |
| --- | --- |
| `POST /pay` | make a payment |
| `GET /payments/{saga_id}` | what really happened (ground truth) |
| `POST /admin/faults` | queue faults for the next payments |
| `POST /admin/config` | `{"seed":42,"fault_rate":0.3,"weights":{"timeout":1,"unknown":2}}`; also resets all state |
| `POST /admin/reset` | clear payments, logs and queue; restart the seed |
| `GET /admin/calls?limit=50` | call log (also printed as JSON lines in `docker compose logs fake-rail`) |
| `GET /admin/stats` | counts by mode and state |

## Useful commands

| Command | What it does |
| --- | --- |
| `docker compose logs -f wallet-service` | follow one service's logs |
| `docker compose down` | stop everything (data kept) |
| `docker compose down -v` | stop and wipe Postgres and Redis data |
| `docker exec -it ledgemind-postgres psql -U ledgemind` | open a SQL shell |
| `docker exec -it ledgemind-redis redis-cli keys "snap:*"` | list wallet snapshots |
