# streetanalyst-ohlcv-downloader

Alpha Vantage `TIME_SERIES_DAILY` → CSV + BigQuery `daily_ohlcv`, once a day, target 20 symbols then probes 21, 22, … until Alpha Vantage's daily limit, run by GitHub Actions. History accumulates with no duplicates (`MERGE` on `symbol, trade_date`).

## Files

| File | Purpose |
|---|---|
| `download_ohlcv.py` | Picks batch, downloads, merges CSVs, loads BigQuery, advances cursor |
| `tickers.txt` | Priority order. Built from Fidelity export Oct-07-2026, sorted by Total gain/loss % (highest first). 233 symbols. |
| `state/cursor.json` | Next position, cycle number, retry list. Committed after every run. |
| `.github/workflows/ohlcv.yml` | Once a day 15:41 UTC (8:41 AM PDT). **Run workflow** button = run now. |
| `data/<SYMBOL>.csv` | Accumulated history per symbol (old + new, dedup on date) |
| `data/combined_ohlcv.csv` | All symbols stacked |
| `data/run_log.csv` | Last run per-symbol status |

## Rotation

| Rule | Behavior |
|---|---|
| Batch | Failed symbols from last run first, then next symbols from cursor; target 20, keeps going until the daily-limit message (cap 100) |
| Wrap | After symbol 233 the cursor returns to 1, `cycle` increments |
| Cycle length | 233 / landed-per-day (≈ 25 on a free key → ≈ 10 days) |
| Daily-limit hit | Stops run; that symbol retries first next run. `state/cursor.json` → `history` logs landed / api_calls / stop_reason per run |
| History depth | Requests `full` (max history). If the key refuses `full`, switches to `compact` (100 days) and re-tests `full` weekly |
| Compact mode | 100-day window per pull; ≈10-day cycle < 100 days → no gaps; history grows forward |

## BigQuery

`besa-capital-financials.financial_data.daily_ohlcv` — created on first run if missing.

| Column | Type |
|---|---|
| symbol | STRING |
| date | DATE |
| open, high, low, close | FLOAT64 |
| volume | INT64 |
| source | STRING |
| loaded_at | TIMESTAMP |

Partitioned by year, clustered by symbol. Each run: load to `daily_ohlcv_staging` → `MERGE` (insert new, update changed) → drop staging.

Dedupe check:
```sql
SELECT symbol, trade_date, COUNT(*) c FROM `besa-capital-financials.financial_data.daily_ohlcv`
GROUP BY 1,2 HAVING c > 1
```
Verification: 0 rows.

## One-time setup

```
gcloud config set project besa-capital-financials
gcloud iam service-accounts create ohlcv-loader
gcloud projects add-iam-policy-binding besa-capital-financials --member=serviceAccount:ohlcv-loader@besa-capital-financials.iam.gserviceaccount.com --role=roles/bigquery.dataEditor
gcloud projects add-iam-policy-binding besa-capital-financials --member=serviceAccount:ohlcv-loader@besa-capital-financials.iam.gserviceaccount.com --role=roles/bigquery.jobUser
gcloud iam service-accounts keys create sa.json --iam-account=ohlcv-loader@besa-capital-financials.iam.gserviceaccount.com

gh auth login
gh secret set ALPHA_VANTAGE_KEY
gh secret set GCP_SA_KEY < sa.json
del sa.json
gh workflow run ohlcv.yml
```
Verification: `gh run list -L 1` → `completed success`; `state/cursor.json` shows `"cursor": 20`.

## Changing the list

Edit `tickers.txt` (order = priority). Cursor keeps its index; new symbols are picked up when the cursor reaches them. To restart from the top: set `"cursor": 0` in `state/cursor.json`.

## Status

| Item | State |
|---|---|
| Rotation, retry, CSV dedupe | Tested offline (13 simulated days, mocked API) |
| BigQuery load/MERGE | Untested (needs `GCP_SA_KEY`) |
| Live Alpha Vantage | Untested |

## Coverage tracking (BigQuery, auto-updating)

Refreshed every run; views recompute on every query (no schedule needed).

| Object | Type | What it answers |
|---|---|---|
| `financial_data.ohlcv_coverage` | View | Per symbol: `first_date` → `last_date`, `days_loaded` vs `expected_days`, `missing_days`, `days_behind_latest`, `status` (NOT LOADED / GAPS / BEHIND / CURRENT) |
| `financial_data.ohlcv_missing_dates` | View | Every `(symbol, missing_date)` gap inside a symbol's loaded range |
| `financial_data.ohlcv_load_log` | Table (append) | Every symbol attempt per run: status, rows, first/last date, outputsize, error |
| `financial_data.ohlcv_universe` | Table (replaced each run) | `tickers.txt` with priority order |

```sql
SELECT * FROM `besa-capital-financials.financial_data.ohlcv_coverage` ORDER BY priority;
SELECT status, COUNT(*) FROM `besa-capital-financials.financial_data.ohlcv_coverage` GROUP BY status;
```
