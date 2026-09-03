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