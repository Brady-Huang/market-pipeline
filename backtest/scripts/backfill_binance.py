"""
從 Binance 公開 REST API 抓取歷史逐筆成交資料 (recent trades),
寫入 TimescaleDB 的 ticks 表,驗證整個管線打通。
"""
import os
import requests
import psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

BINANCE_TRADES_URL = "https://api.binance.com/api/v3/trades"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def fetch_recent_trades(symbol: str, limit: int = 50):
    params = {"symbol": symbol, "limit": limit}
    resp = requests.get(BINANCE_TRADES_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def insert_trades(conn, exchange: str, symbol: str, trades: list):
    insert_sql = """

        INSERT INTO ticks (time, exchange, symbol, trade_id, price, size, side)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time, exchange, symbol, trade_id) DO NOTHING
    """
    rows = []
    for t in trades:
        ts = datetime.fromtimestamp(t["time"] / 1000, tz=timezone.utc)
        side = "sell" if t["isBuyerMaker"] else "buy"
        rows.append((ts, exchange, symbol, t["id"], t["price"], t["qty"], side))

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    conn.commit()
    return len(rows)


if __name__ == "__main__":
    symbol = "BTCUSDT"
    print(f"正在抓取 {symbol} 最近的成交資料...")
    trades = fetch_recent_trades(symbol, limit=50)
    print(f"抓到 {len(trades)} 筆")

    print("連線資料庫並寫入...")
    with psycopg.connect(**DB_CONFIG) as conn:
        inserted = insert_trades(conn, exchange="binance", symbol=symbol, trades=trades)

    print(f"完成,嘗試寫入 {inserted} 筆(重複的會被自動忽略)")