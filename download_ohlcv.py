"""Alpha Vantage OHLCV -> CSV + BigQuery, rotating batches.

Each run:
  1. Reads tickers.txt (order = priority; CUSIPs/dupes skipped).
  2. Picks the batch: failed symbols from last run first, then the next
     BATCH_SIZE symbols from the cursor in state/cursor.json (wraps around).
  3. Downloads TIME_SERIES_DAILY (compact = 100 days, full = all history;
     full requires a premium key).
  4. Writes data/<SYMBOL>.csv, data/combined_ohlcv.csv, data/run_log.csv.
  5. Loads rows to BigQuery staging, MERGEs into daily_ohlcv on (symbol, date)
     -> no duplicates.
  6. Advances the cursor and saves state.

Env:
  ALPHA_VANTAGE_KEY   required
  AV_OUTPUTSIZE       compact (default) | full
  BATCH_SIZE          default 20
  AV_PAUSE_SEC        default 13 (free key 5 req/min)
  BQ_TABLE            default besa-capital-financials.financial_data.daily_ohlcv
  BQ_ENABLED          1 (default) | 0 = CSV only
  GOOGLE_APPLICATION_CREDENTIALS  set by the workflow auth step
"""
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
STATE = ROOT / "state" / "cursor.json"
TICKERS_FILE = ROOT / "tickers.txt"
API_URL = "https://www.alphavantage.co/query"

OUTPUTSIZE = os.environ.get("AV_OUTPUTSIZE", "compact")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
PAUSE_SEC = float(os.environ.get("AV_PAUSE_SEC", "13"))
BQ_TABLE = os.environ.get("BQ_TABLE", "besa-capital-financials.financial_data.daily_ohlcv")
BQ_ENABLED = os.environ.get("BQ_ENABLED", "1") == "1"
FIELDS = ["date", "open", "high", "low", "close", "volume"]
CUSIP_RE = re.compile(r"^(?=.*\d)[0-9A-Z]{9}$")  # 9-char id with a digit = bond/T-bill


# ---------- tickers & state ----------
def load_tickers():
    syms, seen, skipped = [], set(), []
    for line in TICKERS_FILE.read_text(encoding="utf-8").splitlines():
        s = line.split("#", 1)[0].strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        if CUSIP_RE.match(s):
            skipped.append(s)
            continue
        syms.append(s)
    if skipped:
        print(f"SKIP non-equity ids: {skipped}")
    return syms


def load_state():
    if STATE.exists():
        return json.loads(STATE.read_text(encoding="utf-8"))
    return {"cursor": 0, "retry": [], "cycle": 1, "last_run_utc": None}


def save_state(state):
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def pick_batch(tickers, state):
    retry = [s for s in state.get("retry", []) if s in tickers][:BATCH_SIZE]
    batch = list(retry)
    cur = state.get("cursor", 0) % len(tickers)
    cycle = state.get("cycle", 1)
    steps = 0
    while len(batch) < BATCH_SIZE and steps < len(tickers):
        s = tickers[cur]
        if s not in batch:
            batch.append(s)
        cur += 1
        steps += 1
        if cur >= len(tickers):
            cur = 0
            cycle += 1
    return batch, cur, cycle


# ---------- Alpha Vantage ----------
def fetch(symbol, key):
    q = urllib.parse.urlencode({
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": OUTPUTSIZE,
        "datatype": "json",
        "apikey": key,
    })
    with urllib.request.urlopen(f"{API_URL}?{q}", timeout=60) as r:
        payload = json.load(r)
    series = payload.get("Time Series (Daily)")
    if not series:
        msg = payload.get("Note") or payload.get("Information") or payload.get("Error Message") or "no data"
        raise RuntimeError(msg[:200])
    rows = [{
        "date": d,
        "open": v["1. open"],
        "high": v["2. high"],
        "low": v["3. low"],
        "close": v["4. close"],
        "volume": v["5. volume"],
    } for d, v in series.items()]
    rows.sort(key=lambda r: r["date"])
    # Drop today's bar while the US session is open/settling (partial OHLCV).
    from zoneinfo import ZoneInfo
    now_et = dt.datetime.now(ZoneInfo("America/New_York"))
    if now_et.time() < dt.time(16, 30):
        today = now_et.date().isoformat()
        rows = [r for r in rows if r["date"] != today]
    if not rows:
        raise RuntimeError("no completed bars")
    return rows


def is_rate_limited(msg):
    m = msg.lower()
    return "rate limit" in m or "requests per day" in m or "premium" in m


# ---------- CSV ----------
def write_csv(path, fields, rows):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def merge_symbol_csv(sym, new_rows):
    """Keep accumulated history per symbol: old rows + new rows, dedup on date."""
    path = DATA / f"{sym}.csv"
    by_date = {}
    if path.exists():
        with path.open(encoding="utf-8") as f:
            for r in csv.DictReader(f):
                by_date[r["date"]] = r
    for r in new_rows:
        by_date[r["date"]] = r
    write_csv(path, FIELDS, [by_date[d] for d in sorted(by_date)])


def rebuild_combined():
    out = []
    for p in sorted(DATA.glob("*.csv")):
        if p.name in ("combined_ohlcv.csv", "run_log.csv"):
            continue
        with p.open(encoding="utf-8") as f:
            out.extend({"symbol": p.stem, **r} for r in csv.DictReader(f))
    write_csv(DATA / "combined_ohlcv.csv", ["symbol"] + FIELDS, out)
    return len(out)


# ---------- BigQuery ----------
def bq_load(fetched, run_ts):
    from google.cloud import bigquery

    client = bigquery.Client()
    project, dataset, table = BQ_TABLE.split(".")
    staging = f"{project}.{dataset}.{table}_staging"

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{BQ_TABLE}` (
          symbol STRING NOT NULL,
          date DATE NOT NULL,
          open FLOAT64, high FLOAT64, low FLOAT64, close FLOAT64,
          volume INT64,
          source STRING,
          loaded_at TIMESTAMP
        )
        PARTITION BY DATE_TRUNC(date, YEAR)
        CLUSTER BY symbol
    """).result()

    rows = [{
        "symbol": sym, "date": r["date"],
        "open": float(r["open"]), "high": float(r["high"]),
        "low": float(r["low"]), "close": float(r["close"]),
        "volume": int(float(r["volume"])),
        "source": "alphavantage_daily", "loaded_at": run_ts,
    } for sym, rs in fetched.items() for r in rs]
    if not rows:
        return 0, 0

    schema = [
        bigquery.SchemaField("symbol", "STRING"), bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("open", "FLOAT64"), bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"), bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("volume", "INT64"), bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    ]
    job = client.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_TRUNCATE"))
    job.result()

    merge = client.query(f"""
        MERGE `{BQ_TABLE}` T
        USING (
          SELECT * FROM `{staging}`
          WHERE TRUE
          QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY loaded_at DESC) = 1
        ) S
        ON T.symbol = S.symbol AND T.date = S.date
        WHEN MATCHED AND (T.open != S.open OR T.high != S.high OR T.low != S.low
                          OR T.close != S.close OR T.volume != S.volume) THEN
          UPDATE SET open = S.open, high = S.high, low = S.low, close = S.close,
                     volume = S.volume, source = S.source, loaded_at = S.loaded_at
        WHEN NOT MATCHED THEN
          INSERT (symbol, date, open, high, low, close, volume, source, loaded_at)
          VALUES (S.symbol, S.date, S.open, S.high, S.low, S.close, S.volume, S.source, S.loaded_at)
    """)
    merge.result()
    affected = merge.num_dml_affected_rows or 0
    client.query(f"DROP TABLE IF EXISTS `{staging}`").result()
    return len(rows), affected


# ---------- main ----------
def main():
    key = os.environ.get("ALPHA_VANTAGE_KEY")
    if not key:
        sys.exit("ALPHA_VANTAGE_KEY not set")
    DATA.mkdir(exist_ok=True)

    tickers = load_tickers()
    state = load_state()
    batch, next_cursor, next_cycle = pick_batch(tickers, state)
    run_ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Universe={len(tickers)} batch={len(batch)} cursor={state.get('cursor', 0)}->{next_cursor} "
          f"cycle={state.get('cycle', 1)} outputsize={OUTPUTSIZE}")

    fetched, failed, log = {}, [], []
    for i, sym in enumerate(batch):
        if i:
            time.sleep(PAUSE_SEC)
        try:
            rows = fetch(sym, key)
            fetched[sym] = rows
            merge_symbol_csv(sym, rows)
            log.append({"run_utc": run_ts, "symbol": sym, "status": "OK", "rows": len(rows),
                        "first_date": rows[0]["date"], "last_date": rows[-1]["date"], "detail": ""})
            print(f"OK   {sym:<6} {len(rows)} rows  {rows[0]['date']}..{rows[-1]['date']}")
        except Exception as e:
            msg = str(e)
            failed.append(sym)
            log.append({"run_utc": run_ts, "symbol": sym, "status": "FAIL", "rows": 0,
                        "first_date": "", "last_date": "", "detail": msg})
            print(f"FAIL {sym:<6} {msg}")
            if is_rate_limited(msg):
                rest = [s for s in batch[i + 1:]]
                failed.extend(rest)
                print(f"STOP rate limit hit; deferring {rest}")
                break

    write_csv(DATA / "run_log.csv",
              ["run_utc", "symbol", "status", "rows", "first_date", "last_date", "detail"], log)
    total = rebuild_combined()

    staged = merged = 0
    if BQ_ENABLED and fetched:
        staged, merged = bq_load(fetched, run_ts)
        print(f"BQ   staged={staged} merged(inserted+updated)={merged} -> {BQ_TABLE}")

    state.update({"cursor": next_cursor, "cycle": next_cycle, "retry": failed,
                  "last_run_utc": run_ts, "last_ok": len(fetched), "last_fail": len(failed)})
    save_state(state)
    print(f"Done: {len(fetched)}/{len(batch)} OK, csv_rows={total}, retry_next={failed}")
    if not fetched:
        sys.exit(1)


if __name__ == "__main__":
    main()
