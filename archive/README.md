# archive/

One-time load of Steve's old Google Drive price and macro sheets into BigQuery (Oct 7 2026).

| File | BigQuery table | Rows | Notes |
|---|---|---|---|
| price_history_archive.csv.gz | financial_data.price_history_archive | 503,864 | 79 tickers, 1972-2026, one row per symbol+date; GOOGLEFINANCE/Yahoo split-adjusted (see price_basis) |
| macro_series.csv.gz | financial_data.macro_series | 27,424 | VIX, Brent (BZ=F), Dollar Index, Bitcoin, Baker Hughes rig count |

Pushing any change under `archive/` re-runs `archive_load.yml` (WRITE_TRUNCATE, so re-runs never duplicate). `daily_ohlcv` is not touched.
