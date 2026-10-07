"""Alpha Vantage OHLCV -> CSV + BigQuery, rotating batches.

Each run:
  1. Reads tickers.txt (order = priority; CUSIPs/dupes skipped).
  2. Works through failed symbols from last run first, then symbols from the
     cursor in state/cursor.json (wraps around).
  3. Downloads TIME_SERIES_DAILY with outputsize=full (max history). If the key
     refuses full, switches to compact (100 days) for the rest of the run.
  4. Probe mode: keeps going past BATCH_SIZE until Alpha Vantage returns its
     daily-limit message (or MAX_PER_RUN), so each run records how many land.
  5. Writes data/<SYMBOL>.csv, data/combined_ohlcv.csv, data/run_log.csv.
  6. Loads rows to BigQuery staging, MERGEs into daily_ohlcv on (symbol, date)
     -> no duplicates.
  7. Advances the cursor; appends {landed, api_calls, stop_reason, ...} to
     state/cursor.json "history" (last 30 runs).

Env:
  ALPHA_VANTAGE_KEY   required
  AV_OUTPUTSIZE       full (default) | compact
  BATCH_SIZE          default 20 (target)
  PROBE               1 (default) = continue past target until limit | 0 = stop at target
  MAX_PER_RUN         default 100
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

OUTPUTSIZE = os.environ.get("AV_OUTPUTSIZE", "full")   # full = max history; auto-falls back to compact if key isn't premium
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))     # target per run
PROBE = os.environ.get("PROBE", "1") == "1"              # keep going past BATCH_SIZE until the API says stop
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "100"))  # hard cap on attempts per run
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


def queue(tickers, state):
    """Yield (symbol, from_cursor) — retries first, then the rotation from the cursor, one full lap max."""
    retry = [s for s in state.get("retry", []) if s in tickers]
    for s in retry:
        yield s, False
    cur = state.get("cursor", 0) % len(tickers)
    for i in range(len(tickers)):
        s = tickers[(cur + i) % len(tickers)]
        if s not in retry:
            yield s, True


# ---------- Alpha Vantage ----------
class FullNotAllowed(Exception):
    pass


class DailyLimit(Exception):
    pass


def classify(msg):
    m = msg.lower()
    if "outputsize" in m:
        return FullNotAllowed
    if "per day" in m or "daily" in m or "rate limit" in m or "premium" in m:
        return DailyLimit
    return RuntimeError


def fetch(symbol, key, outputsize):
    q = urllib.parse.urlencode({
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": outputsize,
        "datatype": "json",
        "apikey": key,
    })
    with urllib.request.urlopen(f"{API_URL}?{q}", timeout=60) as r:
        payload = json.load(r)
    series = payload.get("Time Series (Daily)")
    if not series:
        msg = payload.get("Note") or payload.get("Information") or payload.get("Error Message") or "no data"
        raise classify(msg)(msg[:300])
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
    run_ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cap = MAX_PER_RUN if PROBE else BATCH_SIZE
    n = len(tickers)
    cursor0, cycle = state.get("cursor", 0) % n, state.get("cycle", 1)
    print(f"Universe={n} target={BATCH_SIZE} probe={PROBE} cap={cap} cursor={cursor0} "
          f"cycle={cycle} outputsize={OUTPUTSIZE}")

    outputsize, fell_back = OUTPUTSIZE, False
    refused = state.get("full_refused_utc")
    if outputsize == "full" and refused:  # don't burn a call on "full" daily; re-test once a week
        age = dt.datetime.now(dt.timezone.utc) - dt.datetime.strptime(refused, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
        if age.days < 7:
            outputsize = "compact"
            print(f"full refused {age.days}d ago -> compact this run (re-test after 7d)")
    fetched, failed, log = {}, [], []
    consumed, calls, stop_reason = 0, 0, "lap_complete"

    def logrow(sym, status, rows=None, detail=""):
        log.append({"run_utc": run_ts, "symbol": sym, "status": status,
                    "rows": len(rows) if rows else 0,
                    "first_date": rows[0]["date"] if rows else "",
                    "last_date": rows[-1]["date"] if rows else "",
                    "outputsize": outputsize, "detail": detail})

    for sym, from_cursor in queue(tickers, state):
        if len(fetched) + len(failed) >= cap:
            stop_reason = "cap"
            break
        if calls:
            time.sleep(PAUSE_SEC)
        if from_cursor:
            consumed += 1
        try:
            calls += 1
            try:
                rows = fetch(sym, key, outputsize)
            except FullNotAllowed as e:
                print(f"NOTE full history refused for this key -> switching to compact: {e}")
                outputsize, fell_back = "compact", True
                state["full_refused_utc"] = run_ts
                time.sleep(PAUSE_SEC)
                calls += 1
                rows = fetch(sym, key, outputsize)
            if outputsize == "full":
                state.pop("full_refused_utc", None)
            fetched[sym] = rows
            merge_symbol_csv(sym, rows)
            logrow(sym, "OK", rows)
            print(f"OK   #{len(fetched):<3} {sym:<6} {len(rows):>5} rows  {rows[0]['date']}..{rows[-1]['date']}")
        except DailyLimit as e:
            failed.append(sym)
            logrow(sym, "LIMIT", detail=str(e))
            print(f"STOP daily limit after {len(fetched)} OK / {calls} calls: {e}")
            stop_reason = "daily_limit"
            break
        except Exception as e:
            failed.append(sym)
            logrow(sym, "FAIL", detail=str(e))
            print(f"FAIL {sym:<6} {e}")

    next_cursor = cursor0 + consumed
    if next_cursor >= n:
        cycle += 1
    next_cursor %= n

    write_csv(DATA / "run_log.csv",
              ["run_utc", "symbol", "status", "rows", "first_date", "last_date", "outputsize", "detail"], log)
    total = rebuild_combined()

    staged = merged = 0
    if BQ_ENABLED and fetched:
        staged, merged = bq_load(fetched, run_ts)
        print(f"BQ   staged={staged} merged(inserted+updated)={merged} -> {BQ_TABLE}")

    run = {"run_utc": run_ts, "landed": len(fetched), "failed": len(failed), "api_calls": calls,
           "stop_reason": stop_reason, "outputsize": outputsize, "full_fallback": fell_back,
           "bq_rows_staged": staged, "bq_rows_merged": merged}
    history = (state.get("history") or [])[-29:] + [run]
    state.update({"cursor": next_cursor, "cycle": cycle, "retry": failed,
                  "last_run_utc": run_ts, "history": history})
    save_state(state)
    print(f"Done: landed={len(fetched)} failed={len(failed)} calls={calls} stop={stop_reason} "
          f"csv_rows={total} retry_next={failed}")
    if not fetched:
        sys.exit(1)


if __name__ == "__main__":
    main()
