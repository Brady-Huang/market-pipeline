"""
驗證 candles 表裡的資料品質:
1. OHLC 邏輯是否一致 (high 最大、low 最小)
2. 是否有負數/零值
3. 時間序列是否連續 (有無缺漏的 K 棒)
"""
import os
import argparse
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# 各 interval 對應的秒數,用來檢查時間連續性
INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def check_ohlc_consistency(conn, symbol: str, interval: str):
    """檢查有沒有 high < 其他價格,或 low > 其他價格的異常列"""
    sql = """
        SELECT time, open, high, low, close
        FROM candles
        WHERE symbol = %s AND interval = %s
          AND (high < open OR high < low OR high < close
               OR low > open OR low > close)
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, interval))
        rows = cur.fetchall()
    return rows


def check_negative_or_zero(conn, symbol: str, interval: str):
    """檢查有沒有價格或成交量 <= 0 的異常列"""
    sql = """
        SELECT time, open, high, low, close, volume
        FROM candles
        WHERE symbol = %s AND interval = %s
          AND (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume < 0)
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, interval))
        rows = cur.fetchall()
    return rows


def check_time_gaps(conn, symbol: str, interval: str):
    """
    用 SQL window function (LEAD) 檢查每一根 K 棒跟下一根之間的時間差,
    找出間隔不等於預期 interval 秒數的地方(代表中間缺資料)。
    """
    expected_seconds = INTERVAL_SECONDS.get(interval)
    if expected_seconds is None:
        print(f"警告:未知的 interval '{interval}',跳過時間連續性檢查")
        return []

    sql = """
        SELECT time, next_time, EXTRACT(EPOCH FROM (next_time - time)) AS gap_seconds
        FROM (
            SELECT time, LEAD(time) OVER (ORDER BY time) AS next_time
            FROM candles
            WHERE symbol = %s AND interval = %s
        ) sub
        WHERE next_time IS NOT NULL
          AND EXTRACT(EPOCH FROM (next_time - time)) != %s
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, interval, expected_seconds))
        rows = cur.fetchall()
    return rows


def run_validation(symbol: str, interval: str):
    with psycopg.connect(**DB_CONFIG) as conn:
        print(f"=== 驗證 {symbol} {interval} 資料 ===\n")

        ohlc_issues = check_ohlc_consistency(conn, symbol, interval)
        print(f"[1] OHLC 邏輯異常: {len(ohlc_issues)} 筆")
        for row in ohlc_issues[:5]:
            print(f"    {row}")

        neg_issues = check_negative_or_zero(conn, symbol, interval)
        print(f"\n[2] 負數/零值異常: {len(neg_issues)} 筆")
        for row in neg_issues[:5]:
            print(f"    {row}")

        gaps = check_time_gaps(conn, symbol, interval)
        print(f"\n[3] 時間缺漏: {len(gaps)} 處")
        for row in gaps[:5]:
            print(f"    缺口: {row[0]} -> {row[1]} (間隔 {row[2]} 秒)")

        total_issues = len(ohlc_issues) + len(neg_issues) + len(gaps)
        print(f"\n=== 總結:發現 {total_issues} 個問題 ===")
        return total_issues


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate candle data quality in TimescaleDB")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m")
    args = parser.parse_args()

    run_validation(args.symbol, args.interval)