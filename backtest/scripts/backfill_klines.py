"""
從 Binance REST API 抓取指定時間範圍的歷史 K 棒資料,
自動分頁抓取(單次最多 1000 筆),寫入 TimescaleDB 的 candles 表。
"""
import os
import time
import argparse
import requests
import psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000  # Binance 單次請求上限

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def to_ms(dt_str: str) -> int:
    """把 'YYYY-MM-DD' 字串轉成 Binance 要的毫秒時間戳"""
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_klines_page(symbol: str, interval: str, start_ms: int, end_ms: int):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": MAX_LIMIT,
    }
    resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def insert_candles(conn, exchange: str, symbol: str, interval: str, klines: list):
    insert_sql = """
        INSERT INTO candles (time, exchange, symbol, interval, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time, exchange, symbol, interval) DO NOTHING
    """
    rows = []
    for k in klines:
        open_time = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        rows.append((open_time, exchange, symbol, interval, k[1], k[2], k[3], k[4], k[5]))

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    conn.commit()
    return len(rows)


def backfill(symbol: str, interval: str, start_date: str, end_date: str, exchange: str = "binance"):
    start_ms = to_ms(start_date)
    end_ms = to_ms(end_date)

    total_inserted = 0
    current_start = start_ms

    with psycopg.connect(**DB_CONFIG) as conn:
        while current_start < end_ms:
            klines = fetch_klines_page(symbol, interval, current_start, end_ms)

            if not klines:
                print("這個時間範圍已經沒有更多資料了,結束。")
                break

            inserted = insert_candles(conn, exchange, symbol, interval, klines)
            total_inserted += inserted

            last_open_time_ms = klines[-1][0]
            first_dt = datetime.fromtimestamp(klines[0][0] / 1000, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(last_open_time_ms / 1000, tz=timezone.utc)
            print(f"抓到 {len(klines)} 筆 ({first_dt} ~ {last_dt}),累計寫入 {total_inserted} 筆")

            # 下一批從最後一根 K 棒的下一個時間點開始,避免重複抓到同一根
            current_start = last_open_time_ms + 1

            # 禮貌性地稍微停一下,避免打太快被 Binance rate limit 擋掉
            time.sleep(0.2)

    print(f"\n完成!總共寫入 {total_inserted} 筆 {symbol} {interval} K 棒資料")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical klines from Binance into TimescaleDB")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--from", dest="start_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="end_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    backfill(args.symbol, args.interval, args.start_date, args.end_date)