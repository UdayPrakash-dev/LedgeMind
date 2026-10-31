@echo off
REM Paper tables and figures from bench\results -> bench\results\paper
docker run --rm -v "%cd%\bench:/bench" -w /bench python:3.12-slim sh -c "pip install -q matplotlib && python aggregate.py %*"
