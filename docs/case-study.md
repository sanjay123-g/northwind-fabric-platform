
## Phase 3: Metadata-driven Bronze ingestion

**Dates:** 2026-11-23 to 2026-11-24

### Business problem
Northwind's data arrives from four systems (marketing, CRM, billing, product telemetry) in different formats (CSV, JSON, Parquet). Writing one pipeline per source does not scale: every new source means new pipeline code, and every format change means a redeploy. The platform needs one generic ingestion process whose behaviour is configured, not hard-coded.

### Approach
A single pipeline, `pl_bronze_ingest`, reads a control table and loops over it:

1. **Lookup (`lkp_sources`)** reads the active rows of `ctl.source_config` in the `wh_gold` warehouse.
2. **ForEach (`fe_sources`)** repeats the load once per source row, sequentially.
3. **Switch (`sw_ingest_method`)** routes each row on its `ingest_method` value: `copy_csv`, `copy_csv_flat`, `copy_json`, `copy_parquet`, or `notebook`.
4. **Default branch** is a Fail activity (`fail_unknown_method`) that stops the run and names the source if a row has an unknown method, so bad metadata is never skipped silently.
5. **Log activities** after the Switch call the stored procedure `ctl.usp_log_run`: `log_success` on the success path and `log_failure` on the failure path, so every source writes one row to `ctl.run_log` per run.

### Control tables (`ctl` schema in `wh_gold`)
- `ctl.source_config`: one row per landing folder (path, format, load type, watermark column, target table, active flag, ingest method).
- `ctl.watermark`: last loaded value per incremental source, with status and run ID (created, not yet written by the pipeline).
- `ctl.run_log`: one row per source per run (run ID, source, start and finish time, rows read and written, status, error message).

### Design decisions and why
- **One watermark store, in the warehouse.** The documented incremental-load pattern is Lookup, Copy, then a Stored Procedure activity that writes the new watermark. Splitting watermark state across a warehouse and a lakehouse table would create two sources of truth.
- **Hybrid ingestion (Copy plus Notebook).** The Copy activity's file format is chosen from a dropdown and the docs do not document it as dynamic, so one Copy cannot serve CSV, JSON and Parquet. Each format gets its own Copy route. Sources with schema drift (`web_sessions`) or nested arrays (`invoices`) go to a Spark notebook (`nb_bronze_loader`) instead.
- **Routing lives in metadata.** Moving a source between paths is one UPDATE on `ctl.source_config`, with no pipeline change.
- **Fail in Default.** An empty Default would skip an unmatched source silently, and the missing data would only surface downstream.
- **Audit columns on every Bronze table.** `_batch_id` (the pipeline run ID) and `_ingested_at` (UTC load time) on all nine tables, so any row traces to one run. The Copy routes add them through the source's Additional columns, and the notebook adds them in code.
- **Logging through a stored procedure.** One `INSERT` lives in `ctl.usp_log_run`, so a log-table change is one edit, not one per activity. Row counts stay NULL for now: they exist only in each Copy's own output, which is out of scope after the Switch.
- **Retries and timeouts per activity.** Retry on the Copy and notebook activities handles transient failures, including Spark capacity rejections; a timeout stops a hung load from holding capacity.
- **Sequential ForEach.** Running notebook iterations in parallel hit the trial capacity limit (HTTP 430, no queueing). Sequential removed the contention.

### Problems found and how they were solved
- **Partition columns missing in Bronze.** Hive-style folders (`snapshot_date=...`) keep the partition value in the folder name, not in the file. Copy ignores it by default. Fix: enable partition discovery with the root path left empty (typed root paths failed with an invalid-root-path error).
- **Flat folder failing partition discovery.** `campaigns` has no partition folders, so discovery fails. Fix: a separate `copy_csv_flat` route without discovery.
- **Doubled path on JSON and Parquet.** The Container setting is prefixed to the folder path. Fix: drop `Files/landing/` from the folder expression for those routes.
- **Notebook parameters ignored.** Base parameters only reach the cell tagged as the parameter cell. With the cell untagged, the notebook ran with its defaults and stamped `manual` as the batch ID. Fix: toggle the parameter cell.
- **Spark capacity rejection (430) on the notebook step.** An open interactive session counted against the same trial capacity. Fix: sequential loop, and stop the notebook session before pipeline runs.

### Status
- All nine Bronze tables load through the pipeline; the last full run succeeded for all nine sources with nine `Succeeded` rows in `ctl.run_log`.
- Audit columns confirmed on all nine tables. `_batch_id` shows the pipeline run ID on the Copy-route tables; the two notebook tables still need one pipeline rerun to confirm the run ID (the parameter cell was only tagged after the last run).
- Not yet built: watermark updates, Overwrite to incremental load, handling of the one `web_sessions` file with a drifted schema, and a run-level failure alert (planned for the orchestration phase).

### Skills demonstrated
Metadata-driven pipeline design, Fabric Data Factory (Lookup, ForEach, Switch, Copy, Notebook, Stored procedure, Fail, dynamic expressions, retries), Hive-style partition handling, T-SQL control tables and stored procedures in a Fabric warehouse, Spark capacity behaviour, and working from the platform docs to find limits before designing around them.
