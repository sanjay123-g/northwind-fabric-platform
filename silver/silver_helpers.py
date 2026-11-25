"""
silver_helpers.py - shared Silver-layer helpers for the Northwind Fabric platform.

validate_and_split(bronze_df, contract) applies one source's data contract to a Bronze
DataFrame and returns (valid_df, quarantine_df):
  1. trims text and turns empty strings into NULL
  2. casts every contract column to its Silver type with try_cast (a bad value becomes NULL, never an error)
  3. finds the first problem per row: CAST_FAILED, NULL_KEY / INVALID_VALUE (required column empty,
     value not in allowed_values, failed 'expr' rule)
  4. removes duplicate keys (keeps the newest row) -> DUPLICATE
Rows with a problem go to the quarantine DataFrame with a reason code; the rest are valid.
Every Bronze row ends up in exactly one of the two (see reconcile).
"""
import json
from pyspark.sql import functions as F, Window


def load_contract(path, source):
    """Read the contracts JSON file and return the contract for one source."""
    with open(path) as f:
        contracts = json.load(f)
    contract = dict(contracts["sources"][source])
    contract["source"] = source
    return contract


def _try_cast(colname, dtype):
    # try_cast returns NULL for a value that does not fit the type. A plain cast can raise an error
    # when ANSI mode is on, so try_cast is the safe choice on every Spark version.
    return F.expr(f"try_cast(`{colname}` as {dtype})")


def validate_and_split(df, contract):
    source = contract["source"]
    cols = contract["columns"]
    pk = contract["primary_key"]
    order_col = contract.get("dedupe_order_by", "_ingested_at")

    # Keep the untouched Bronze row as JSON, so a quarantined row can always be traced back.
    work = df.withColumn("_raw_json", F.to_json(F.struct(*[F.col(c) for c in df.columns])))

    # Step 1: trimmed text (empty string -> NULL) in _raw_<name>; a column missing from Bronze becomes NULL.
    for c in cols:
        n = c["name"]
        text = F.trim(F.col(n).cast("string")) if n in df.columns else F.lit(None).cast("string")
        work = work.withColumn(f"_raw_{n}", F.when(text == "", F.lit(None)).otherwise(text))

    # Step 2: typed columns. The select keeps contract columns, audit columns and the raw helpers.
    typed = []
    for c in cols:
        n, t = c["name"], c["type"]
        typed.append((F.col(f"_raw_{n}") if t == "string" else _try_cast(f"_raw_{n}", t)).alias(n))
    raw_cols = [F.col(f"_raw_{c['name']}") for c in cols]
    audit = [F.col("_batch_id").cast("string").alias("_batch_id"),
             _try_cast("_ingested_at", "timestamp").alias("_ingested_at")]
    if "_source_file" in df.columns:
        audit.append(F.col("_source_file"))
    work = work.select(*typed, *raw_cols, *audit, F.col("_raw_json"))

    # Step 3: list of (condition that means "bad row", reason code, detail), in priority order.
    problems = []
    for c in cols:                                   # a value was present but did not cast
        n = c["name"]
        problems.append((F.col(f"_raw_{n}").isNotNull() & F.col(n).isNull(), "CAST_FAILED", f"{n} cannot be cast to {c['type']}"))
    for c in cols:                                   # a required column is empty
        n = c["name"]
        if not c["nullable"]:
            code = "NULL_KEY" if (n in pk or n.endswith("_id")) else "INVALID_VALUE"   # a missing identifier vs another missing value
            problems.append((F.col(n).isNull(), code, f"{n} is required"))
    for c in cols:                                   # value outside the allowed list
        n = c["name"]
        if c.get("allowed_values"):
            problems.append((F.col(n).isNotNull() & ~F.col(n).isin(*c["allowed_values"]), "INVALID_VALUE", f"{n} not in allowed values"))
    for ch in contract["checks"]:                    # custom rules written as Spark SQL
        if "expr" in ch and ch["action"] == "quarantine":
            problems.append((~F.coalesce(F.expr(ch["expr"]), F.lit(True)), ch["reason"], ch["name"]))

    first_problem = F.coalesce(*[
        F.when(cond, F.struct(F.lit(code).alias("code"), F.lit(detail).alias("detail")))
        for cond, code, detail in problems])
    work = work.withColumn("_dq", first_problem)

    bad = work.filter(F.col("_dq").isNotNull())
    ok = work.filter(F.col("_dq").isNull())

    # Step 4: duplicates among the good rows - keep the newest row per key, quarantine the rest.
    w = Window.partitionBy(*pk).orderBy(F.col(order_col).desc_nulls_last())
    ranked = ok.withColumn("_rn", F.row_number().over(w))
    dupes = (ranked.filter("_rn > 1").drop("_rn")
             .withColumn("_dq", F.struct(F.lit("DUPLICATE").alias("code"),
                                         F.lit("duplicate on " + ",".join(pk)).alias("detail"))))
    valid_cols = [c["name"] for c in cols] + ["_batch_id", "_ingested_at"] + (["_source_file"] if "_source_file" in df.columns else [])
    valid = ranked.filter("_rn = 1").select(*valid_cols)

    quarantine = bad.unionByName(dupes).select(
        F.lit(source).alias("source"),
        F.col("_dq.code").alias("reason_code"),
        F.col("_dq.detail").alias("reason_detail"),
        F.col("_raw_json").alias("raw_record"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("quarantined_at"))
    return valid, quarantine


def warn_counts(valid_df, contract):
    """Count rows in the valid data that break a 'warn' rule (rows are kept, only counted)."""
    rules = [ch for ch in contract["checks"] if "expr" in ch and ch["action"] == "warn"]
    if not rules:
        return {}
    row = valid_df.agg(*[
        F.sum(F.when(~F.coalesce(F.expr(r["expr"]), F.lit(True)), 1).otherwise(0)).alias(r["name"])
        for r in rules]).collect()[0]
    return {r["name"]: int(row[r["name"]] or 0) for r in rules}


def reconcile(bronze_df, valid_df, quarantine_df):
    """Row-count reconciliation: Bronze rows must equal valid rows plus quarantined rows."""
    n_in, n_ok, n_bad = bronze_df.count(), valid_df.count(), quarantine_df.count()
    by_reason = {r["reason_code"]: r["count"] for r in quarantine_df.groupBy("reason_code").count().collect()}
    return {"rows_in": n_in, "rows_valid": n_ok, "rows_quarantined": n_bad,
            "balanced": n_in == n_ok + n_bad, "quarantine_by_reason": by_reason}
