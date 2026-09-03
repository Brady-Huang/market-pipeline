# Distributed Market Data Pipeline & Backtester

> A high-concurrency, event-driven market data ingestion and vectorized backtesting system for crypto & equities, built with Go and Python.

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go)](https://go.dev/)
[![Python Version](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python)](https://www.python.org/)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL-fdb515?logo=postgresql)](https://www.timescale.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Key Engineering Highlights](#key-engineering-highlights)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Development Roadmap](#development-roadmap)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [Testing](#testing)
- [Performance Benchmarks](#performance-benchmarks)
- [Lessons Learned](#lessons-learned)

---

## Overview

This project implements a production-style pipeline that:

1. Ingests real-time market data (trades / order book updates) from multiple exchanges (Binance, Coinbase) via WebSocket, at high concurrency.
2. Cleans, deduplicates, and writes normalized tick data into a time-series database (TimescaleDB).
3. Provides a vectorized backtesting engine (Python) so users can plug in a strategy and get a full PnL / risk report.

Built to demonstrate distributed-systems and financial-data-engineering skills relevant to quant infra, prop trading, and Web3 market-making roles: reconnection handling, backpressure, data integrity, and time-series storage optimization.

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│  Exchange Layer                                              │
│  Binance WS / Coinbase WS  (≥20 trading pairs)                │
└───────────────┬────────────────────────────────────────────┘
                │  WebSocket (AggTrade / OrderBook diff)
┌───────────────▼────────────────────────────────────────────┐
│  Go Ingestion Service                                         │
│  - Goroutine per exchange connection                          │
│  - Exponential backoff reconnect                               │
│  - REST API gap-filler on reconnect                             │
│  - Redis-based idempotent dedup (Symbol+Timestamp+TradeID)      │
└───────────────┬────────────────────────────────────────────┘
                │  Batched writes
┌───────────────▼────────────────────────────────────────────┐
│  TimescaleDB                                                  │
│  - Hypertables (1-day chunks)                                 │
│  - Compression policy (>7 days → columnar)                    │
└───────────────┬────────────────────────────────────────────┘
                │  Historical query
┌───────────────▼────────────────────────────────────────────┐
│  Python Analytics / Backtest Engine                            │
│  - Vectorized strategy execution (Pandas / Polars)              │
│  - Asyncio-driven multi-strategy parallel runs                  │
│  - PnL, Sharpe, Max Drawdown reporting                          │
└────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Go (Goroutines + Channels) | High-concurrency WebSocket handling |
| Analytics / Backtest | Python 3.11+ (Asyncio, Pandas/Polars, NumPy) | Vectorized strategy backtesting |
| Storage | TimescaleDB (PostgreSQL) | Time-series tick/candle storage |
| Dedup / Cache | Redis | Idempotency keys, distributed locks |
| Infra | Docker & Docker Compose | Local orchestration |
| CI/CD | GitHub Actions | Automated testing & build validation |

---

## Key Engineering Highlights

### 1. Reconnection & Data Integrity
- **Exponential backoff with jitter** to avoid reconnection storms.
- **REST API gap-filler**: on reconnect, computes the missing time window and backfills via REST candle/trade endpoints.
- **Idempotent writes**: `ON CONFLICT DO NOTHING` on `(time, exchange, symbol, trade_id)` prevents duplicate ticks during reconnect windows.

### 2. High-Concurrency Ingestion (Go)
- One goroutine per exchange connection, isolated with `recover()` so a single feed crash doesn't take down the pipeline.
- Bounded channels for backpressure — when downstream write throughput can't keep up, the system degrades explicitly rather than growing memory unbounded.
- Heartbeat monitoring with active reconnect on missed pings, instead of waiting on TCP timeout.

### 3. Time-Series Storage Optimization
- **Hypertables** partitioned by time (1-day chunks) to keep write performance flat as data grows.
- **Compression policy** on data older than 7 days, converting to columnar storage (~70%+ disk savings, faster historical scans).

### 4. Vectorized Backtesting (Python)
- No `iterrows()` — all signal generation and PnL calculation done via vectorized Pandas/Polars operations.
- **Look-ahead bias prevention**: signals are explicitly `shift(1)`-ed before being used for execution, so no trade is placed on information not yet available at that timestamp.
- Asyncio orchestration allows multiple parameter sets / strategies to be backtested concurrently.

### 5. Numeric Precision
- Prices/sizes stored as `NUMERIC(24, 8)`, never raw `float64`, to avoid floating-point drift compounding across large tick volumes.
- All timestamps normalized to UTC with timezone-aware storage (`TIMESTAMPTZ`).

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Go 1.22+ (for Phase 2 onward)

### 1. Clone

```bash
git clone https://github.com/YOUR_USERNAME/market-pipeline.git
cd market-pipeline
```

### 2. Spin up TimescaleDB + Redis

```bash
cd infra
docker-compose up -d
```

Verify both containers are healthy:

```bash
docker ps
```

### 3. Initialize the database schema

```bash
docker exec -i market_timescaledb psql -U market_data -d market_data < timescaledb/init.sql
```

### 4. Set up Python environment

```bash
cd ../backtest
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Configure environment variables

Create a `.env` file in the project root (never commit this):

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=market_data
DB_USER=market_data
DB_PASSWORD=market_dev_pw
```

### 6. Run the historical backfill script

```bash
python scripts/backfill_binance.py
```

This fetches recent trades from Binance's public REST API and writes them into the `ticks` hypertable.

### 7. (Coming in Phase 2) Run the Go ingestion service

```bash
cd ../ingestion
go run cmd/main.go --exchanges=binance,coinbase
```

### 8. (Coming in Phase 4) Run a backtest

```bash
cd ../backtest
python run_backtest.py --strategy ma_cross --symbol BTCUSDT --from 2025-01-01 --to 2025-06-01
```

> Full setup instructions, environment variables, and API key configuration in [`docs/SETUP.md`](docs/SETUP.md).

---

## Project Structure

```
.
├── ingestion/              # Go WebSocket ingestion service
│   ├── cmd/
│   ├── internal/
│   │   ├── exchange/       # Per-exchange WS clients (Binance, Coinbase)
│   │   ├── reconnect/      # Backoff + gap-filler logic
│   │   ├── dedup/          # Redis idempotency layer
│   │   └── storage/        # TimescaleDB writer
│   └── go.mod
├── backtest/                # Python vectorized backtest engine
│   ├── scripts/              # One-off / backfill scripts
│   │   └── backfill_binance.py
│   ├── strategies/           # Pluggable strategy modules
│   ├── engine/                # Core backtest + reporting logic
│   └── requirements.txt
├── infra/
│   ├── docker-compose.yml
│   └── timescaledb/          # Hypertable + compression setup scripts
│       └── init.sql
├── .github/workflows/ci.yml
├── .gitignore
└── docs/
    ├── SETUP.md
    └── ARCHITECTURE.md
```

---

## Development Roadmap

- [x] **Phase 1 — Storage & Validation**
  - [x] Docker Compose setup for TimescaleDB + Redis
  - [x] Define Hypertable schema (`ticks`)
  - [x] Python script: pull recent Binance trades via REST, write to `ticks`
  - [ ] Extend script to backfill an arbitrary historical date range
  - [ ] Add data validation (schema checks, outlier detection)

- [ ] **Phase 2 — Go High-Concurrency Ingestion**
  - [ ] WebSocket client per exchange (Binance first, then Coinbase)
  - [ ] Goroutine pool + bounded channel write pipeline
  - [ ] Batched writes to TimescaleDB

- [ ] **Phase 3 — Fault Tolerance & Optimization**
  - [ ] Exponential backoff reconnect
  - [ ] REST API gap-filler on reconnect
  - [ ] Redis idempotent dedup
  - [ ] Hypertable compression policy

- [ ] **Phase 4 — Backtest Engine & CI/CD**
  - [ ] Vectorized backtest engine (MA cross strategy as reference implementation)
  - [ ] Look-ahead bias safeguards + unit tests
  - [ ] GitHub Actions: unit tests, Docker build validation
  - [ ] Finalize README + architecture diagram

---

## Design Decisions & Trade-offs

| Decision | Why | Trade-off Accepted |
|---|---|---|
| Go for ingestion, not Python | Goroutines handle thousands of concurrent WS connections without GIL contention | Two languages to maintain instead of one |
| Vectorized backtest, not event-driven | 100x+ faster iteration for strategy research | Must explicitly guard against look-ahead bias; harder to model slippage/partial fills realistically |
| TimescaleDB over InfluxDB | SQL-native, strong compression, good for relational joins with trade metadata | Less write-throughput ceiling than purpose-built TSDBs at extreme scale |
| `ON CONFLICT DO NOTHING` over Redis dedup (Phase 1) | Simpler for the initial REST-based backfill; DB-level unique constraint is sufficient at this stage | Will move hot-path dedup to Redis in Phase 3 once WebSocket ingestion is high-throughput |

---

## Testing

- **Unit tests**: reconnect/backoff logic (simulated disconnects), dedup key generation, signal-shift correctness in backtest engine.
- **Integration tests**: Docker Compose stack boots, ingestion writes reach TimescaleDB, backtest engine reads back correctly.
- **CI**: GitHub Actions runs Go + Python test suites and a Docker build check on every PR.

---

## Performance Benchmarks

> Fill in with real numbers once implemented — this section is what interviewers actually want to see.

- Sustained ingestion throughput: `___` ticks/sec across `___` symbols with zero dropped messages
- Reconnect recovery time (avg): `___` ms
- Backtest runtime: `___` years of tick data in `___` seconds (vectorized) vs `___` seconds (naive loop baseline)
- Compression ratio achieved on >7-day-old data: `___`%

---

## Lessons Learned

> A short section (2–4 bullets) written *after* building — this is often the first thing an interviewer reads.
- Postgres only applies `POSTGRES_USER` / `POSTGRES_PASSWORD` env vars the *first* time a volume is initialized — if you change credentials in `docker-compose.yml` later, you need `docker-compose down -v` to actually reset them.
- (Add more as you go: what broke first when you simulated a disconnect mid-stream, measured impact of `float64` vs `Decimal`, exchange rate limits in practice, etc.)

---

## License

MIT