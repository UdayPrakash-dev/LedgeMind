@echo off
REM LedgeMind benchmark. Examples:
REM   bench.bat --n 10 --seed 1            (smoke test)
REM   bench.bat --n 100 --seed 1           (full run)
REM   bench.bat --n 100 --seed 2 --no-throughput
docker compose --profile bench run --rm bench %*
