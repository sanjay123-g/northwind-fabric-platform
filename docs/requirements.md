# Requirements summary: Northwind Cloud revenue intelligence platform

## Client
Northwind Cloud (fictitious B2B SaaS) sells annual subscriptions to mid-market customers in four regions:
North America West, North America East, EMEA, APAC. Prospects arrive through paid marketing, content and a
self-serve free trial. Sales works opportunities in a CRM, billing runs in a separate subscription system, and
product usage is captured as event telemetry.

## Problem
Leadership gets three different revenue numbers depending on who they ask. Marketing, sales and finance each
pull from their own system. Reports take days to reconcile, pipeline forecasts are not trusted, and nobody can
see which campaigns produce paying customers. There is no early warning when trial signups drop or when a
customer's usage falls off before renewal.

## Business requirements
1. One governed source of truth for revenue, pipeline, marketing spend and product usage.
2. Daily refreshed reporting: funnel conversion, customer acquisition cost, pipeline velocity, net revenue
   retention, churn risk.
3. Near-real-time visibility of trial signups and product usage, with automated alerts on anomalies.
4. Regional sales reps see only their own territory; deal amounts and cost data restricted to approved roles.
5. Pipelines are reliable, observable and recoverable without manual data fixes.
6. Full documentation so the client's team can operate the platform after handover.

## Success criteria (record real numbers in the README as they are measured)
- Pipeline success rate of 99% or higher across the test period.
- Data quality pass rate reported per table; bad rows quarantined, never silently dropped.
- Daily refresh completes within an agreed window (record actual time).
- Query time improvement from OPTIMIZE and V-Order (record before and after).
- Reruns are idempotent: running any load twice produces identical results.

## Sources
| Source | Content | Type |
|---|---|---|
| Marketing | campaigns, ad spend, web sessions | batch files (CSV, Parquet) |
| CRM | accounts, contacts, opportunities | relational source, loaded by pipeline |
| Billing | subscriptions, invoices, invoice lines | per playbook; format to be decided in Phase 2 |
| Product telemetry | usage events, trial signups | streaming |

## Assumptions
- Source systems are simulated with synthetic data generated in Fabric.
- Capacity is a shared F64 trial; heavy jobs are batched.
- The trial expires around mid-December, so build and evidence capture finish before then.
