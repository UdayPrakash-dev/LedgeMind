# LedgeMind

An event-sourced wallet ledger whose stuck transfers (sagas) are repaired by an LLM that picks from a fixed action menu, with a deterministic verifier approving every fix before it runs.

## Layout

```
docker-compose.yml     runs Postgres, Redis, wallet-service, recovery-service
db/init/               SQL run once when the Postgres volume is first created
wallet-service/        Go: event store, wallets, transfers, sagas
recovery-service/      Python: AI planner, verifier, resolution memory (Day 6+)
fake-rail/             fake payment service with fault injection (Day 4)
bench/                 benchmark harness (Day 8)
paper/                 LaTeX source
```

## Run locally (Windows, PowerShell)

Prerequisites: Docker Desktop, Go 1.24+, Git.

```powershell
copy .env.example .env                  # first time only
cd wallet-service; go mod tidy; cd ..   # first time only: downloads Go packages, creates go.sum
docker compose up --build -d            # start everything
docker compose ps                       # all four should be "running" / "healthy"
curl http://localhost:8080/health       # {"service":"wallet","status":"ok"}
curl http://localhost:8000/health       # {"service":"recovery","status":"ok",...}
```

## Run the event-store tests

Needs Postgres from the step above.

```powershell
cd wallet-service
$env:DATABASE_URL = "postgres://ledgemind:ledgemind@localhost:5432/ledgemind?sslmode=disable"
go test -v ./...
```

macOS/Linux: `export DATABASE_URL=...` instead of `$env:DATABASE_URL = ...`.

## Useful commands

| Command | What it does |
| --- | --- |
| `docker compose logs -f wallet-service` | follow one service's logs |
| `docker compose down` | stop everything (data kept) |
| `docker compose down -v` | stop and wipe the database (re-runs `db/init`) |
| `docker exec -it ledgemind-postgres psql -U ledgemind` | open a SQL shell |
