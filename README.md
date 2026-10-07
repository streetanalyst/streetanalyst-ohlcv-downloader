# streetanalyst-ohlcv-downloader

Alpha Vantage `TIME_SERIES_DAILY` → CSV + BigQuery `daily_ohlcv`, 20 symbols per day in rotating batches, run by GitHub Actions. History accumulates with no duplicates (`MERGE` on `symbol, date`).

## Files

| File | Purpose |
|---|---|
| `download_ohlcv.py` | Picks batch, downloads, merges CSVs, loads BigQuery, advances cursor |
| `tickers.txt` | Priority order. Built from Fidelity export Oct-07-2026, sorted by Total gain/loss % (highest first). 233 symbols. |
| `state/cursor.json` | Next position, cycle number, retry list. Committed after every run. |
| `.github/workflows/ohlcv.yml` | Daily 01:17 UTC (6:17 PM PDT). Manual run button. |
| `data/<SYMBOL>.csv` | Accumulated history per symbol (old + new, dedup on date) |
| `data/combined_ohlcv.csv` | All symbols stacked |
| `data/run_log.csv` | Last run per-symbol status |

## Rotation

| Rule | Behavior |
|---|---|
| Batch | Failed symbols from last run first, then next symbols from cursor, total 20 |
| Wrap | After symbol 233 the cursor returns to 1, `cycle` increments |
| Cycle length | 233 / 20 ≈ 12 days |
| Rate-limit hit | Stops batch, remaining symbols go to retry list |
| Free key (compact) | 100-day window per pull; 12-day cycle < 100 days → no gaps; history grows forward |
| Premium key | Set `AV_OUTPUTSIZE: "full"` in the workflow → max history per symbol, same dedupe |

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
SELECT symbol, date, COUNT(*) c FROM `besa-capital-financials.financial_data.daily_ohlcv`
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
