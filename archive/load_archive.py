"""One-time loader: Drive price/macro archives -> BigQuery (WRITE_TRUNCATE, idempotent).
price_history_archive: one row per symbol+trade_date (deduped before upload).
macro_series: one row per series_id+obs_date.
Neither touches daily_ohlcv."""
from google.cloud import bigquery as bq
P = "besa-capital-financials.financial_data."
c = bq.Client(project="besa-capital-financials")
S = bq.SchemaField
jobs = {
  "price_history_archive": ("archive/price_history_archive.csv.gz", [
    S("symbol","STRING","REQUIRED"), S("trade_date","DATE","REQUIRED"), S("open","FLOAT64"), S("high","FLOAT64"),
    S("low","FLOAT64"), S("close","FLOAT64","REQUIRED"), S("adj_close","FLOAT64"), S("volume","FLOAT64"),
    S("price_basis","STRING"), S("source_file","STRING"), S("source_file_id","STRING")]),
  "macro_series": ("archive/macro_series.csv.gz", [
    S("series_id","STRING","REQUIRED"), S("series_name","STRING"), S("frequency","STRING"), S("obs_date","DATE","REQUIRED"),
    S("open","FLOAT64"), S("high","FLOAT64"), S("low","FLOAT64"), S("close","FLOAT64","REQUIRED"), S("volume","FLOAT64"),
    S("extra","FLOAT64"), S("source_file","STRING"), S("source_file_id","STRING")]),
}
for t, (path, schema) in jobs.items():
    cfg = bq.LoadJobConfig(schema=schema, source_format=bq.SourceFormat.CSV, skip_leading_rows=1,
                           write_disposition="WRITE_TRUNCATE", allow_quoted_newlines=True)
    with open(path, "rb") as f:
        job = c.load_table_from_file(f, P + t, job_config=cfg)
    job.result()
    print(t, "rows:", c.get_table(P + t).num_rows)
