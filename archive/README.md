# archive/

One-time load of Steve's old Google Drive price and macro sheets into BigQuery (Oct 7 2026).

| File | BigQuery table | Rows | Notes |
|---|---|---|---|
| price_history_archive.csv.gz | financial_data.price_history_archive | 503,864 | 79 tickers, 1972-2026, one row per symbol+date; GOOGLEFINANCE/Yahoo split-adjusted (see price_basis) |
| macro_series.csv.gz | financial_data.macro_series | 27,424 | VIX, Brent (BZ=F), Dollar Index, Bitcoin, Baker Hughes rig count |
| gas_prices_la.csv.gz | financial_data.gas_prices_la | weekly | EIA weekly Los Angeles regular retail gasoline, Jun 2000 - Apr 2024 (kept separate from macro_series) |

Pushing any change under `archive/` re-runs `archive_load.yml` (WRITE_TRUNCATE, so re-runs never duplicate). `daily_ohlcv` is not touched.

## ticker_lists
Membership only (list_id, list_type, symbol, source file). 32 lists: blacklists, index constituents (R2K, SP500, NASDAQ, NYSE, NYSE foreign, DOW, OTCQB), universes, portfolios, screens, watchlist. No descriptions, sectors or AI text. Source sheets kept in Drive (live). View `ticker_list_check` = one row per symbol with blacklist/index/portfolio flags.

## sheet_cell_archive
Lossless cell-by-cell copy (displayed values, non-empty cells) of list workbooks trashed 2026-10-12. One row per cell: file, tab, row, col, value. `contains_ai_text` = TRUE for the 360GPT / 360 Industry / 2024 Corporate Profile files (ChatGPT-written descriptions; archive only, never use as labels). Rebuild any tab by pivoting row_num × col.
