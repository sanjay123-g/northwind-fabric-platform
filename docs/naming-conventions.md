# Naming conventions
Playbook examples first; the rest follow the same pattern.

| Item | Pattern | Example |
|---|---|---|
| Lakehouse | lh_<layer> | lh_bronze, lh_silver |
| Warehouse | wh_<layer> | wh_gold |
| Notebook | nb_<layer>_<entity> | nb_silver_accounts |
| Pipeline | pl_<role>_<cadence or entity> | pl_master_daily |
| Tables | <layer prefix or source>_<entity>, snake_case | crm_accounts, fact_subscription_revenue |
| Audit columns (bronze) | _ingested_at, _source_file, _batch_id | |
