-- 啟用 TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 逐筆成交資料表
CREATE TABLE IF NOT EXISTS ticks (
    time        TIMESTAMPTZ       NOT NULL,
    exchange    TEXT              NOT NULL,
    symbol      TEXT              NOT NULL,
    trade_id    BIGINT            NOT NULL,
    price       NUMERIC(24, 8)    NOT NULL,
    size        NUMERIC(24, 8)    NOT NULL,
    side        TEXT              NOT NULL CHECK (side IN ('buy', 'sell')),
    PRIMARY KEY (time, exchange, symbol, trade_id)
);

-- 轉成 Hypertable,以 1 天為一個 chunk
SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 常用查詢索引:依 exchange + symbol + 時間 查詢
CREATE INDEX IF NOT EXISTS idx_ticks_exchange_symbol_time ON ticks (exchange, symbol, time DESC);


-- K 棒資料表
CREATE TABLE IF NOT EXISTS candles (
    time        TIMESTAMPTZ       NOT NULL,
    exchange    TEXT              NOT NULL,
    symbol      TEXT              NOT NULL,
    interval    TEXT              NOT NULL,   -- e.g. '1m', '5m', '1h'
    open        NUMERIC(24, 8)    NOT NULL,
    high        NUMERIC(24, 8)    NOT NULL,
    low         NUMERIC(24, 8)    NOT NULL,
    close       NUMERIC(24, 8)    NOT NULL,
    volume      NUMERIC(24, 8)    NOT NULL,
    PRIMARY KEY (time, exchange, symbol, interval)
);

-- 轉成 Hypertable
SELECT create_hypertable('candles', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 常用查詢索引
CREATE INDEX IF NOT EXISTS idx_candles_exchange_symbol_interval_time ON candles (exchange, symbol, interval, time DESC);