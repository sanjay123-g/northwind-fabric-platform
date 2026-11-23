# 001 Gold layer: Warehouse vs Lakehouse
Status: DRAFT, to confirm and own before interviews.

## Context
Gold serves a star schema to the semantic model and must enforce regional row-level security for sales reps
(requirement 4).

## Options
1. Warehouse (T-SQL, multi-table transactions, native RLS/CLS/DDM in T-SQL)
2. Lakehouse (Spark-first, Delta tables, SQL analytics endpoint read-only)

## Decision (proposed)
Warehouse for gold; lakehouses for bronze and silver.

## Why
T-SQL-based modelling and multi-table transactions suit a star schema; row-level security is a T-SQL feature
on warehouse objects. Spark users can still read gold.

## Trade-offs
Two engines to operate; Spark writes into the warehouse are not the native path.
