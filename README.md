# Alpaca Trading Bot

## Background

This codebase represents work I contributed to during my time as a Python Developer at a remote trading platform startup (2020–2021). The startup was building automated trading systems with the goal of eliminating human intervention from the trading process entirely.

After leaving the role, I reconstructed and cleaned up my personal contributions into this standalone repository. The architecture reflects the real system design we worked with — real-time WebSocket data ingestion, second-level data aggregation, multi-session market awareness, and rule-based order execution via the Alpaca brokerage API.

This is not a tutorial bot or a simple API wrapper. It is a structured, production-oriented automated trading system built with real market data and live order execution.

---

## What It Does

The bot connects to live US stock market data, monitors price movements in real time, evaluates configurable trading strategies (formulas), and executes buy/sell orders automatically through the Alpaca brokerage API.

It supports three market sessions — pre-market, normal hours, and after-hours — and handles the full order lifecycle: market orders, limit orders, stop-limit orders, and sell status tracking.

---

## Architecture

### `trader.py` — Order Execution Layer
Abstract base class `Trader` defines the full order execution interface. `AlpakaTrader` implements it against the Alpaca REST API.

Supported order types:
- Market buy/sell
- Limit buy/sell
- Stop-limit buy/sell
- Order status tracking (filled/pending)

### `strategy.py` — Strategy Framework & Signal Engine
Core of the system. Contains:
- `MarketHoursCalifornia` — defines pre-market (01:00–06:29), normal (06:30–12:59), and after-market (13:00–16:59) session windows
- `BuySellEvents` — state machine managing the full buy→hold→sell lifecycle per symbol
- Ban mode — automatically blacklists symbols after a loss to prevent repeated bad trades
- Multiple formula variants (Formula 1, 3, 4) with configurable ban/no-ban modes for strategy comparison
- Persistent buy/sell event logging to JSON for post-session analysis

### `data_processor.py` — Data Aggregation Pipeline
- `AggregateMaker` — converts raw second-level tick data into minute or hour aggregates
- `TimePipe` — sliding time window that retains only recent data within a configurable lookback period, discarding stale ticks automatically
- `TimeRangeCreator` — generates time range dictionaries for session boundary comparisons

### `stock_websocket_api.py` — Real-Time Data Feed
- `WebSocketClientClone` — custom async WebSocket client running in a dedicated thread, receiving live tick data from Polygon's market data stream
- `WebSocketConnectionManager` — manages multiple simultaneous WebSocket connections
- Designed for continuous streaming — reconnects on disconnect, processes messages via callback pattern

### `stock_data.py` — Market Data Layer
- Integration with Polygon API for real-time stock data
- Real-time data storage for multiple WebSocket clients
- Top-20 detector for identifying most active symbols

### `custom_time.py` — Timezone Handling
- `CustomTimeZone` — handles conversion between machine time, UTC, New York market time, and Los Angeles client time
- Millisecond timestamp conversion for WebSocket tick data alignment

---

## Trading Data

`non_otc_symbols_2021_08_01.csv` — A curated list of non-OTC (over-the-counter) tradeable symbols as of August 2021. Used to filter the bot's universe to legitimate exchange-listed stocks only, avoiding illiquid or manipulated OTC markets.

---

## Strategy Testing

Multiple formula variants are included for strategy comparison:

| File | Description |
|------|-------------|
| `formula1_test_no_ban.py` | Formula 1 — ban mode disabled |
| `formula1_test_no_ban_with_cancel.py` | Formula 1 — ban disabled, order cancellation enabled |
| `formula1_test_yes_ban.py` | Formula 1 — ban mode enabled |
| `formula3_test_no_ban.py` | Formula 3 — ban mode disabled |
| `formula3_test_yes_ban.py` | Formula 3 — ban mode enabled |
| `formula4_test_no_ban.py` | Formula 4 — ban mode disabled |
| `formula4_test_yes_ban.py` | Formula 4 — ban mode enabled |

Ban mode prevents re-entry into a symbol after a loss within the same session.

---

## APIs Used

| API | Purpose |
|-----|---------|
| Alpaca Trade API | Order execution — buy, sell, account balance, order status |
| Polygon.io | Real-time market data stream via WebSocket |

---

## Tech Stack

- **Language:** Python
- **Brokerage:** Alpaca Trade API
- **Market Data:** Polygon.io WebSocket stream
- **Concurrency:** Python threading for WebSocket and strategy loops
- **Data:** pandas · JSON persistence for trade logs
- **Time Handling:** pytz · python-dateutil

---

## Important Notes

This repository reflects my personal contribution to a proprietary trading system. Proprietary strategy parameters, credentials, and live system configuration are not included. The `creds.py` file contains placeholder credential classes only — actual API keys must be supplied via environment variables.

This code is shared for portfolio and reference purposes. It is not intended as financial advice and should not be used for live trading without thorough testing and risk management controls.

---

## Author

**Tanjil Hasan**
github.com/tanjilh136 · linkedin.com/in/tanjil-hasan-650246169
