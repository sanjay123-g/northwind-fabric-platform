# 002 Shortcuts vs copying data into bronze
Status: DRAFT, to confirm and own before interviews.

## Context
Sources are simulated and land in a landing zone. Bronze rows need audit columns (ingested time, source file,
batch ID) and watermark-based incremental loads.

## Options
1. OneLake shortcuts to the landing zone (no copy, pointer only)
2. Copy into bronze with a metadata-driven pipeline

## Decision (proposed)
Copy into bronze.

## Why
Audit columns, watermark control and idempotent reruns require owning the bronze data. A shortcut is a pointer,
so it cannot add columns or isolate bronze from changes at the source.

## Trade-offs
Extra storage and copy time; the gain is control and reproducibility.
