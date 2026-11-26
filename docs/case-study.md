
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
- Audit columns confirmed on all nine tables. `_batch_id` shows the pipeline run ID (a GUID) on every table, including the two notebook-loaded tables, after the parameter cell was tagged and the pipeline rerun.
- Not yet built: watermark updates, Overwrite to incremental load, handling of the one `web_sessions` file with a drifted schema, and a run-level failure alert (planned for the orchestration phase).

### Skills demonstrated
Metadata-driven pipeline design, Fabric Data Factory (Lookup, ForEach, Switch, Copy, Notebook, Stored procedure, Fail, dynamic expressions, retries), Hive-style partition handling, T-SQL control tables and stored procedures in a Fabric warehouse, Spark capacity behaviour, and working from the platform docs to find limits before designing around them.

---

## Phase 4: Silver engineering framework

### What it does
Turns raw Bronze into typed, validated, deduplicated Silver tables, and keeps every rejected row in a quarantine table with a reason code. One shared helper module and one data contract per source drive all nine tables, so the same code handles every source.

### Design decisions
- **Data contracts.** `contracts/silver_contracts.json` defines, per source: columns and types, primary key, allowed values, and checks with an action (`quarantine` or `warn`). Silver reads the contract, so adding a rule is a config change, not a code change.
- **Cast and validate in Silver, not Bronze.** Bronze stays raw and replayable (mostly text from Copy). `try_cast` turns a bad value into NULL instead of failing the job, and the original row is kept as JSON in quarantine for traceability and lineage.
- **Quarantine instead of drop.** Each rejected row carries a reason code (`NULL_KEY`, `DUPLICATE`, `INVALID_VALUE`, `FK_MISSING`, `CAST_FAILED`). Duplicates keep the newest row.
- **Reconcile every load.** Rows in Bronze must equal valid rows plus quarantined rows. The result is saved per source in `slv_load_audit`.
- **Idempotent writes with Delta MERGE on the primary key.** Reruns give the same result. Append would duplicate on a retry; overwrite breaks once a batch is partial; delete-then-insert is not atomic. Log tables (`slv_load_audit`, `slv_dq_results`) append on purpose because each run is a new fact.
- **Invoice lines.** The nested `lines` array in invoices is exploded into `slv_invoice_lines` (key `invoice_id` + `line_id`), using the newest copy of each invoice and only validated invoices.
- **SCD type 2 for accounts.** `slv_account_history` keeps every version of an account with `valid_from`, `valid_to`, `is_current`. A new version starts only when tier, status or owner changes (detected with a row hash against the previous snapshot). Country and industry never changed in the data, so they are not tracked. Rebuilt in full from the retained snapshots with window functions, which is idempotent and simple at this size; a large dimension or a change-only feed would use an incremental MERGE instead.
- **Referential checks.** Orphan counts for 8 parent-child relationships are saved per run in `slv_dq_results`.
- **Maintenance.** OPTIMIZE runs on every Silver table at the end of the load. VACUUM stays at the 7-day default, per Microsoft's guidance on shorter retention affecting time travel and concurrent readers.

### Results (first full load)
- All nine sources balanced (rows in = valid + quarantined). The packaged `nb_silver_load` notebook ran end to end and its final check passed.
- Seeded dirty data caught: `usage_events` 82,946 of 4,921,393 rows quarantined (24,718 null key, 48,409 duplicates, 9,819 invalid values); `invoices` 1,109 duplicates; `subscriptions` 727 duplicates; `opportunities` 362 invalid values.
- `slv_invoice_lines`: 52,712 rows; rerunning gives the same count.
- Referential checks: 0 orphans across all 8 relationships.
- SCD2: 21,621 version rows for 20,000 accounts (20,000 current); 1,621 accounts have more than one version.
- Late-arriving events: 96,360 of 4,838,447 usage events (about 2%) arrived 1 to 5 days late.
- OPTIMIZE compacted `slv_usage_events` from 9 files to 1.
- Partition test: partitioning a copy of `slv_usage_events` by `ingest_date` produced 362 files instead of 1 and a one-day query of 0.39 s vs 0.48 s, within noise at 85 MB. Decision: leave it unpartitioned and revisit if the table grows to hundreds of GB.

### Problems found and how they were solved
- **Wrong reason code for missing keys.** A required `_id` column that was NULL was reported as `INVALID_VALUE`. Found by the local test suite; fixed so required id columns report `NULL_KEY`.
- **Misread VACUUM dry run.** Counting the rows of `VACUUM ... DRY RUN` returned 1, which looked like one file to delete. Listing the output showed it was the table's metadata folder, so there were no old data files.
- **Silver tables landing in the Bronze lakehouse.** The notebook's default lakehouse was `lh_bronze`, so Silver tables were created there. Resolved: Silver now has its own lakehouse, `lh_silver` (see below).

### Status
- Done: contracts, shared helper module, quarantine, reconciliation, idempotent MERGE for all nine sources, invoice lines, SCD2 accounts, referential checks, OPTIMIZE, partition test, packaged framework notebook.
- Open decisions (not decided yet): late-arriving data policy (flag only, reprocessing window, or cut-off), Bronze Overwrite vs Append (blocks the watermark and incremental load), and the `web_sessions` schema-drift policy. Decided: Silver gets its own lakehouse (below).
- Pipeline orchestration done: `pl_bronze_ingest` runs `nb_silver_load` after Bronze ingestion, passing the pipeline run ID as `batch_id`.
- Not yet built: watermark and incremental load, handling of the `web_sessions` schema-drift file.

### Layer separation: Silver in its own lakehouse
- **Layout:** `lh_bronze` (raw), `lh_silver` (clean), `wh_gold` (warehouse for modeled data). This follows Microsoft's medallion guidance of lakehouses for Bronze and Silver and a warehouse for Gold. A single lakehouse with schemas is also a documented option, but separate items give each layer its own boundary for access control and clear lineage.
- **Cross-lakehouse read:** a Spark notebook reads tables from its default lakehouse only. `nb_silver_load` has `lh_silver` as default and reads Bronze through its OneLake path (`abfss://<workspace id>@onelake.dfs.fabric.microsoft.com/<lakehouse id>/Tables/dbo/<table>`) via a small `bronze()` helper, rather than relying on cross-lakehouse default-lakehouse behavior.
- **Config files** (`silver_contracts.json`, `silver_helpers.py`) live in `lh_silver` Files/config, because `/lakehouse/default` points at the default lakehouse.
- **Verified:** after the move, all nine sources loaded and balanced with identical counts, and 14 `slv_*` tables exist in `lh_silver`.
