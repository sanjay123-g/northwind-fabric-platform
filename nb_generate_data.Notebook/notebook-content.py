# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "031b0202-314e-4fa8-a994-e2fccfc85bea",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "82b494dc-d048-486d-9dc1-61aed01f62a3",
# META       "known_lakehouses": [
# META         {
# META           "id": "031b0202-314e-4fa8-a994-e2fccfc85bea"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "55b3492d-e454-b29d-4c0c-c7a225cadb80",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_generate_data (v4) - Northwind Cloud: a coherent B2B SaaS business, simulated
# One simulated company, 9 source tables across 4 systems. The tables obey business rules so that Gold metrics mean something:
# - Marketing: campaigns -> ad_spend (daily) -> web_sessions
# - CRM: accounts (acquired by a campaign) -> contacts -> opportunities (closed-won matches a subscription, booked at a discount)
# - Billing: subscriptions (seats, plan, cadence, churn, upgrades) -> invoices with nested lines (monthly/annual, priced from the plan)
# - Telemetry: usage_events only while an account is active, proportional to seats
# Planted on purpose: duplicates, nulls, invalid values, late events, one schema-drift file, CRM-vs-billing amount mismatch.
# Same SEED + same Spark partitioning => same data.

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_bronze",
# MAGIC         "workspaceId": "82b494dc-d048-486d-9dc1-61aed01f62a3"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS (Fabric: cell menu > Toggle parameter cell)
import os
from datetime import date, timedelta
SEED = 42
N_ACCOUNTS = 20000
N_CAMPAIGNS = 120
N_USAGE_EVENTS = 5_000_000
N_WEB_SESSIONS = 1_000_000
HIST_START = "2025-11-22"
HIST_DAYS = 365
EXTRACT1_DAY = 240          # day number of the first billing extract; the second extract is incremental after it
PAID_SHARE = 0.75           # share of accounts that become paying
OVERWRITE = True

BASE = os.environ.get("NW_BASE")                      # local testing only
if BASE is None:                                      # Fabric: absolute OneLake path of the DEFAULT lakehouse
    ctx = notebookutils.runtime.context
    if not ctx.get("defaultLakehouseId"):
        raise RuntimeError("No default lakehouse. Run the %%configure cell first, then this cell.")
    BASE = (f"abfss://{ctx['currentWorkspaceId']}@onelake.dfs.fabric.microsoft.com/"
            f"{ctx['defaultLakehouseId']}/Files/landing")
_hs = date.fromisoformat(HIST_START)
HIST_END = (_hs + timedelta(days=HIST_DAYS - 1)).isoformat()
E1 = (_hs + timedelta(days=EXTRACT1_DAY)).isoformat()          # also the first CRM snapshot date
DRIFT_DAY = (_hs + timedelta(days=100)).isoformat()
print("Writing to:", BASE, "| history", HIST_START, "->", HIST_END, "| extract1", E1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# IMPORTS + HELPERS
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
MODE = "overwrite" if OVERWRITE else "errorifexists"
_n = [0]
def sd():
    _n[0] += 1
    return SEED + _n[0]

def dlit(s): return F.lit(s).cast("date")
START, END = dlit(HIST_START), dlit(HIST_END)

def pick(values, seed):
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.floor(F.rand(seed) * len(values)) + 1).cast("int"))

def pickw(pairs, seed):
    """Weighted choice: pickw([("a", .7), ("b", .3)], seed)"""
    r, tot, cum, e = F.rand(seed), sum(w for _, w in pairs), 0.0, None
    for v, w in pairs[:-1]:
        cum += w / tot
        e = F.when(r < cum, F.lit(v)) if e is None else e.when(r < cum, F.lit(v))
    return e.otherwise(F.lit(pairs[-1][0]))

def by(col, mapping):
    """Map a column's values through a python dict (as a CASE WHEN)."""
    items = list(mapping.items())
    e = F.when(col == items[0][0], F.lit(items[0][1]))
    for k, v in items[1:]:
        e = e.when(col == k, F.lit(v))
    return e

def pid(prefix, col, width=7):
    return F.concat(F.lit(prefix), F.lpad(col.cast("string"), width, "0"))

def write(df, path, fmt="csv", part=None):
    w = df.write.mode(MODE).format(fmt)
    if fmt == "csv":
        w = w.option("header", True)
    if part:
        w = w.partitionBy(part)
    w.save(f"{BASE}/{path}")

INDUSTRIES = ["Retail", "Finance", "Healthcare", "Manufacturing", "Media", "Logistics", "Education"]
COUNTRIES = ["CA", "US", "UK", "DE", "AU", "IN"]
OWNERS = ["owner_a", "owner_b", "owner_c", "owner_d", "owner_e"]
CHANNEL_W = [("Paid Search", .28), ("Social", .22), ("Email", .12), ("Webinar", .10), ("Organic", .18), ("Partner", .10)]
CAC = {"Paid Search": 6500, "Social": 5000, "Email": 900, "Webinar": 2800, "Organic": 300, "Partner": 3500}
CPM = {"Paid Search": 35, "Social": 20, "Email": 8, "Webinar": 15, "Organic": 10, "Partner": 25}
PRICE_M = {"Starter": 15, "Team": 29, "Business": 49, "Enterprise": 89}      # list price per seat per month
AI_PRICE_M = 12
ANNUAL_FACTOR = 0.9                                                           # annual prepay = 12 months at 90%
SEATS_LO = {"Starter": 3, "Team": 8, "Business": 20, "Enterprise": 60}
SEATS_HI = {"Starter": 10, "Team": 30, "Business": 80, "Enterprise": 250}
CHURN_H = {"Starter": .04, "Team": .025, "Business": .015, "Enterprise": .006}  # monthly churn hazard
ANNUAL_P = {"Starter": .2, "Team": .3, "Business": .5, "Enterprise": .8}
NEXT_PLAN = {"Starter": "Team", "Team": "Business", "Business": "Enterprise", "Enterprise": "Enterprise"}
TIER = {"Starter": "Pro", "Team": "Pro", "Business": "Enterprise", "Enterprise": "Enterprise"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 1: campaigns (shape only; budgets are set after accounts exist, so spend is tied to who was acquired)
camp = (spark.range(N_CAMPAIGNS).select(
    F.col("id").alias("cidx"),
    pid("C", F.col("id"), 4).alias("campaign_id"),
    pickw(CHANNEL_W, sd()).alias("channel"),
    F.concat(F.lit("Campaign "), F.col("id").cast("string")).alias("campaign_name"),
    F.date_add(START, F.floor(F.lit(HIST_DAYS * 0.9) * F.sqrt(F.rand(sd()))).cast("int")).alias("start_date"),   # more campaigns later = growth
    (F.rand(sd()) * 60 + 30).cast("int").alias("duration_days")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 2: accounts = the master truth for the whole company (kept in memory, written later as CRM snapshots)
a = spark.range(N_ACCOUNTS).select(
    F.col("id").alias("aid"),
    F.when(F.rand(sd()) < 0.2, F.lit(None).cast("long")).otherwise(F.floor(F.rand(sd()) * N_CAMPAIGNS)).alias("cidx"))   # 20% organic, no campaign
a = a.join(F.broadcast(camp.select("cidx", "campaign_id", "channel", "start_date", "duration_days")), "cidx", "left")
days_avail = F.datediff(END, F.col("start_date"))
camp_signup = F.date_add(F.col("start_date"),
                         F.floor(F.rand(sd()) * (F.least(F.col("duration_days") + 30, days_avail) + 1)).cast("int"))
org_signup = F.date_add(START, F.floor(F.lit(HIST_DAYS - 1) * F.sqrt(F.rand(sd()))).cast("int"))
a = a.select(
    "aid", "campaign_id", "channel",
    F.when(F.col("campaign_id").isNotNull(), camp_signup).otherwise(org_signup).alias("signup_date"),
    pickw([("Starter", .35), ("Team", .35), ("Business", .20), ("Enterprise", .10)], sd()).alias("plan"),
    (F.rand(sd()) < PAID_SHARE).alias("wants_paid"),
    F.rand(sd()).alias("r_seats"), F.rand(sd()).alias("r_cad"), F.rand(sd()).alias("r_trial"),
    F.greatest(F.rand(sd()), F.lit(1e-9)).alias("r_churn"), F.rand(sd()).alias("r_ai"),
    F.rand(sd()).alias("r_chg"), F.rand(sd()).alias("r_chg2"), F.rand(sd()).alias("r_chgm"),
    F.rand(sd()).alias("r_owner"), pick(OWNERS, sd()).alias("owner0"), pick(OWNERS, sd()).alias("owner1"),
    pick(INDUSTRIES, sd()).alias("industry"), pick(COUNTRIES, sd()).alias("country"))
P = F.col("plan")
a = (a.withColumn("seats", F.floor(by(P, SEATS_LO) + F.col("r_seats") * (by(P, SEATS_HI) - by(P, SEATS_LO) + 1)).cast("int"))
      .withColumn("cadence", F.when(F.col("r_cad") < by(P, ANNUAL_P), "annual").otherwise("monthly"))
      .withColumn("sub_start", F.date_add(F.col("signup_date"), (7 + F.floor(F.col("r_trial") * 15)).cast("int")))
      .withColumn("is_paid", F.col("wants_paid") & (F.col("sub_start") <= END)))
h = by(P, CHURN_H)
life = F.greatest(F.lit(1), F.floor(F.log(F.col("r_churn")) / F.log(1 - h)))
life = F.when(F.col("cadence") == "annual", F.ceil(life / 12) * 12).otherwise(life).cast("int")   # annual: churn only at renewal
a = (a.withColumn("life_m", life)
      .withColumn("end_cand", F.add_months(F.col("sub_start"), F.col("life_m")))
      .withColumn("end_date", F.when(F.col("is_paid") & (F.col("end_cand") <= END), F.col("end_cand")))
      .withColumn("has_ai", F.col("is_paid") & (P != "Starter") & (F.col("r_ai") < 0.3))
      .withColumn("has_support", P.isin("Business", "Enterprise")))
# one amendment per subscription at most: expansion 15%, upgrade 5%, contraction 4%. Applied at a billing-period boundary.
ctype = F.when(F.col("r_chg") < .15, "expansion").when(F.col("r_chg") < .20, "upgrade").when(F.col("r_chg") < .24, "contraction")
ctype = F.when((ctype == "upgrade") & (P == "Enterprise"), "expansion").otherwise(ctype)
cdate = F.add_months(F.col("sub_start"), F.when(F.col("cadence") == "annual", 12).otherwise((2 + F.floor(F.col("r_chgm") * 9)).cast("int")))
valid = F.col("is_paid") & ctype.isNotNull() & (cdate <= END) & (F.col("end_date").isNull() | (cdate < F.col("end_date")))
a = (a.withColumn("change_type", F.when(valid, ctype))
      .withColumn("change_date", F.when(valid, cdate))
      .withColumn("plan_after", F.when(valid, F.when(F.col("change_type") == "upgrade", by(P, NEXT_PLAN)).otherwise(P)))
      .withColumn("seats_after", F.when(valid, F.when(F.col("change_type") == "expansion", F.ceil(F.col("seats") * (1.3 + F.col("r_chg2") * 0.7)))
                                         .when(F.col("change_type") == "contraction", F.greatest(F.lit(1), F.floor(F.col("seats") * 0.7)))
                                         .otherwise(F.col("seats"))).cast("int"))
      .withColumn("account_id", pid("A", F.col("aid") + 1, 6))
      .withColumn("subscription_id", F.when(F.col("is_paid"), pid("SUB", F.col("aid") + 1, 6))))
acct = a.cache()
n_paid = acct.filter("is_paid").count()
print("accounts:", N_ACCOUNTS, "| paying:", n_paid, "| churned in window:", acct.filter("end_date is not null").count(),
      "| with amendment:", acct.filter("change_date is not null").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 3: MARKETING: campaigns (budget tied to accounts won), ad_spend (daily), web_sessions
won = acct.groupBy("campaign_id").agg(F.count("*").alias("n_won"))
camp_b = (camp.join(won, "campaign_id", "left").withColumn("n_won", F.coalesce("n_won", F.lit(0)))
          .withColumn("budget", F.round(by(F.col("channel"), CAC) * F.col("n_won") * (0.6 + F.rand(sd()) * 0.9)
                                        + 2000 + F.rand(sd()) * 18000, 2))
          .withColumn("end_date", F.date_add(F.col("start_date"), F.col("duration_days") - 1)).cache())
write(camp_b.select("campaign_id", "channel", "campaign_name", "start_date", "end_date", "budget"), "marketing/campaigns")

spend = (camp_b.select("campaign_id", "channel", "start_date", "duration_days", "budget",
                       F.explode(F.sequence(F.lit(0), F.col("duration_days") - 1)).alias("d"))
         .withColumn("spend_date", F.date_add(F.col("start_date"), F.col("d")))
         .filter(F.col("spend_date") <= END)
         .withColumn("spend", F.round(F.col("budget") / F.col("duration_days") * (0.7 + F.rand(sd()) * 0.6), 2))
         .withColumn("impressions", (F.col("spend") * 1000 / by(F.col("channel"), CPM)).cast("long"))
         .withColumn("clicks", (F.col("impressions") * (0.005 + F.rand(sd()) * 0.025)).cast("long"))
         .select("campaign_id", "spend_date", "spend", "impressions", "clicks"))
write(spend, "marketing/ad_spend", part="spend_date")

tot_budget = camp_b.agg(F.sum("budget")).first()[0]
sess = (camp_b.select("campaign_id", "start_date", "duration_days",
                      F.floor(F.col("budget") / F.lit(tot_budget) * N_WEB_SESSIONS + F.rand(sd())).cast("int").alias("n"))
        .filter("n >= 1")
        .select("campaign_id", "start_date", "duration_days", F.explode(F.sequence(F.lit(1), F.col("n"))).alias("k"))
        .select(pid("S", F.monotonically_increasing_id(), 14).alias("session_id"),
                F.date_add(F.col("start_date"), F.floor(F.rand(sd()) * (F.least(F.col("duration_days"), F.datediff(END, F.col("start_date"))) + 1)).cast("int")).alias("session_date"),
                F.col("campaign_id").alias("utm_campaign"),
                pick(COUNTRIES, sd()).alias("country"),
                F.when(F.rand(sd()) < 0.3, pid("A", (F.rand(sd()) * N_ACCOUNTS).cast("int") + 1, 6)).alias("account_id"),   # NULL = anonymous
                (F.rand(sd()) * 12 + 1).cast("int").alias("pages_viewed")))
write(sess.filter(F.col("session_date") != DRIFT_DAY), "marketing/web_sessions", part="session_date")
# SCHEMA DRIFT: one day's file renamed country->geo and added device_type. Bronze ingestion must survive this.
(sess.filter(F.col("session_date") == DRIFT_DAY).withColumnRenamed("country", "geo")
     .withColumn("device_type", pick(["desktop", "mobile", "tablet"], sd())).drop("session_date")
     .write.mode(MODE).option("header", True).csv(f"{BASE}/marketing/web_sessions/session_date={DRIFT_DAY}"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 4: CRM: accounts (2 snapshots), contacts, opportunities
def plan_asof(d):   # plan active on date d (None if not subscribed then)
    active = F.col("is_paid") & (F.col("sub_start") <= d) & (F.col("end_date").isNull() | (d < F.col("end_date")))
    return F.when(active, F.when(F.col("change_date").isNotNull() & (F.col("change_date") <= d), F.col("plan_after")).otherwise(F.col("plan")))

def snapshot(d_str):
    d = dlit(d_str)
    pl = plan_asof(d)
    status = (F.when(pl.isNotNull(), "active")
              .when(F.col("is_paid") & F.col("end_date").isNotNull() & (F.col("end_date") <= d), "churned")
              .when(F.col("is_paid") & (F.col("sub_start") > d), "trial").otherwise("free"))
    owner = F.when(F.col("r_owner") < 0.06, F.col("owner1")).otherwise(F.col("owner0")) if d_str == HIST_END else F.col("owner0")
    return (acct.filter(F.col("signup_date") <= d).select(
        "account_id", F.concat(F.lit("Company "), (F.col("aid") + 1).cast("string")).alias("account_name"),
        "industry", "country",
        F.coalesce(F.when(pl.isin("Starter", "Team"), "Pro").when(pl.isin("Business", "Enterprise"), "Enterprise"), F.lit("Free")).alias("tier"),
        status.alias("account_status"), owner.alias("owner"),
        F.col("signup_date").alias("created_date"), F.col("campaign_id").alias("acquisition_campaign_id"),
        d.alias("updated_at")))
write(snapshot(E1), f"crm/accounts/snapshot_date={E1}")
write(snapshot(HIST_END), f"crm/accounts/snapshot_date={HIST_END}")

contacts = (spark.range(N_ACCOUNTS * 3).select(
    pid("P", F.col("id"), 7).alias("contact_id"),
    pid("A", (F.rand(sd()) * N_ACCOUNTS).cast("int") + 1, 6).alias("account_id"),
    F.when(F.rand(sd()) < 0.03, F.lit(None)).when(F.rand(sd()) < 0.02, F.lit("not-an-email"))
     .otherwise(F.concat(F.lit("user"), F.col("id").cast("string"), F.lit("@example.com"))).alias("email"),
    pick(["Admin", "Analyst", "Engineer", "Executive", "Finance"], sd()).alias("job_role"),
    F.rand(sd()).alias("r"))
    .join(F.broadcast(acct.select("account_id", "signup_date")), "account_id")
    .withColumn("created_date", F.least(F.date_add(F.col("signup_date"), (F.col("r") * 20).cast("int")), END))
    .drop("r", "signup_date"))
write(contacts, f"crm/contacts/snapshot_date={HIST_END}")

# Opportunities: one closed-won per subscription (booked at a discount to list), one per expansion/upgrade, plus lost/open noise
def annual_list(seats, plan, ai, support, cad):
    base = seats * (by(plan, PRICE_M) + F.when(ai, F.lit(AI_PRICE_M)).otherwise(F.lit(0))) * 12
    base = F.when(cad == "annual", base * ANNUAL_FACTOR).otherwise(base)
    return base + F.when(support, seats * by(plan, PRICE_M) * 12 * 0.10).otherwise(F.lit(0.0))
disc = pickw([(0.0, .5), (0.05, .25), (0.10, .15), (0.15, .10)], sd())
acv0 = annual_list(F.col("seats"), F.col("plan"), F.col("has_ai"), F.col("has_support"), F.col("cadence"))
acv1 = annual_list(F.col("seats_after"), F.col("plan_after"), F.col("has_ai"), F.col("has_support"), F.col("cadence"))
mismatch = F.when(F.rand(sd()) < 0.04, 1.25).otherwise(1.0)       # PLANTED: ~4% of CRM bookings overstated vs billing
o_new = (acct.filter("is_paid").select("account_id", F.lit("Closed Won").alias("stage"),
         F.round(acv0 * (1 - disc) * mismatch, 2).alias("amount"),
         F.date_sub(F.col("sub_start"), (F.rand(sd()) * 7).cast("int")).alias("close_date"),
         F.lit("new").alias("opportunity_type")))
o_exp = (acct.filter(F.col("change_type").isin("expansion", "upgrade")).select("account_id", F.lit("Closed Won").alias("stage"),
         F.round((acv1 - acv0) * (1 - disc), 2).alias("amount"),
         F.date_sub(F.col("change_date"), (F.rand(sd()) * 7).cast("int")).alias("close_date"),
         F.lit("expansion").alias("opportunity_type")))
o_noise = (spark.range(N_ACCOUNTS).select(
         pid("A", (F.rand(sd()) * N_ACCOUNTS).cast("int") + 1, 6).alias("account_id"),
         pick(["Prospect", "Qualified", "Proposal", "Closed Lost"], sd()).alias("stage"),
         F.round(F.rand(sd()) * 90000 + 1000, 2).alias("amount"),
         F.date_add(START, (F.rand(sd()) * HIST_DAYS).cast("int")).alias("close_date"),
         F.lit("new").alias("opportunity_type")))
opps = (o_new.unionByName(o_exp).unionByName(o_noise)
        .withColumn("created_date", F.date_sub(F.col("close_date"), (20 + F.rand(sd()) * 60).cast("int")))
        .withColumn("amount", F.when(F.rand(sd()) < 0.01, -F.abs(F.col("amount"))).otherwise(F.col("amount")))   # invalid negative amounts
        .withColumn("opportunity_id", pid("O", F.monotonically_increasing_id(), 12))
        .select("opportunity_id", "account_id", "stage", "amount", "close_date", "created_date", "opportunity_type"))
write(opps, f"crm/opportunities/snapshot_date={HIST_END}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 5: BILLING (JSON, as a billing API would export): subscriptions + invoices with nested lines
subs = acct.filter("is_paid")

def sub_asof(d_str):
    d = dlit(d_str)
    s = subs.filter(F.col("sub_start") <= d)
    chg = F.col("change_date").isNotNull() & (F.col("change_date") <= d)
    ended = F.col("end_date").isNotNull() & (F.col("end_date") <= d)
    return s.select(
        "subscription_id", "account_id",
        F.when(chg, F.col("plan_after")).otherwise(F.col("plan")).alias("plan"),
        F.when(chg, F.col("seats_after")).otherwise(F.col("seats")).alias("seats"),
        F.col("cadence").alias("billing_cadence"), F.col("sub_start").alias("start_date"),
        F.when(ended, F.col("end_date")).alias("end_date"),
        F.when(ended, "cancelled").otherwise("active").alias("status"),
        F.when(chg, F.col("change_type")).alias("last_change_type"),
        F.when(chg, F.col("change_date")).alias("last_change_date"),
        F.greatest(F.col("sub_start"), F.when(chg, F.col("change_date")), F.when(ended, F.col("end_date"))).cast("timestamp").alias("modified_at"))

# invoice periods: monthly => one per month, annual => one per year, from start until churn/end of history
step = F.when(F.col("cadence") == "annual", 12).otherwise(1)
mb = F.floor(F.months_between(END, F.col("sub_start"))).cast("int")
nper = F.when(F.col("end_date").isNull(), F.floor(mb / step) + 1).otherwise((F.col("life_m") / step).cast("int"))
inv = (subs.select("subscription_id", "account_id", "plan", "seats", "plan_after", "seats_after", "change_date", "has_ai", "has_support",
                   "cadence", "sub_start", step.alias("step"), nper.alias("nper"))
       .filter("nper >= 1")
       .select("*", F.explode(F.sequence(F.lit(0), F.col("nper") - 1)).alias("k"))
       .withColumn("period_start", F.add_months(F.col("sub_start"), F.col("k") * F.col("step")))
       .withColumn("period_end", F.date_sub(F.add_months(F.col("sub_start"), (F.col("k") + 1) * F.col("step")), 1))
       .withColumn("applied", F.col("change_date").isNotNull() & (F.col("period_start") >= F.col("change_date")))
       .withColumn("p", F.when(F.col("applied"), F.col("plan_after")).otherwise(F.col("plan")))
       .withColumn("s", F.when(F.col("applied"), F.col("seats_after")).otherwise(F.col("seats")))
       .withColumn("unit", by(F.col("p"), PRICE_M) * F.when(F.col("cadence") == "annual", 12 * ANNUAL_FACTOR).otherwise(1.0))
       .withColumn("ai_unit", F.lit(AI_PRICE_M) * F.when(F.col("cadence") == "annual", 12 * ANNUAL_FACTOR).otherwise(1.0))
       .withColumn("invoice_id", pid("INV", F.monotonically_increasing_id(), 12))
       .withColumn("invoice_date", F.col("period_start")))
def line(name, cond, qty, unit):
    return F.when(cond, F.struct(F.lit(name).alias("product"), qty.cast("int").alias("quantity"),
                                 F.round(unit, 2).alias("unit_price"),
                                 F.round(qty * unit, 2).alias("amount"),
                                 F.col("period_start").alias("period_start"), F.col("period_end").alias("period_end")))
lines = F.filter(F.array(
        line("Platform", F.lit(True), F.col("s"), F.col("unit")),
        line("AI Add-on", F.col("has_ai"), F.col("s"), F.col("ai_unit")),
        line("Support", F.col("has_support"), F.lit(1), F.col("s") * F.col("unit") * 0.10)), lambda x: x.isNotNull())
lines = F.transform(lines, lambda x, i: x.withField("line_id", F.concat(F.col("invoice_id"), F.lit("-L"), (i + 1).cast("string")))
                                          .withField("quantity", F.when(F.abs(F.hash(F.col("invoice_id"), i)) % 200 == 0, F.lit(-1)).otherwise(x["quantity"])))   # PLANTED: qty != amount/price on ~0.5% of lines
pay_lag = F.floor(F.rand(sd()) * F.when(F.col("p") == "Enterprise", 35).otherwise(12)).cast("int")
r_st = F.rand(sd())
inv = (inv.withColumn("lines", lines)
          .withColumn("total_amount", F.round(F.aggregate(F.col("lines"), F.lit(0.0), lambda acc, x: acc + x["amount"]), 2))
          .withColumn("pay_date", F.date_add(F.col("invoice_date"), pay_lag))
          .withColumn("final_status", F.when(r_st < 0.01, "void").when(r_st < 0.035, "past_due")
                                       .when(F.col("pay_date") > END, "open").otherwise("paid"))
          .withColumn("modified_at", F.when(F.col("final_status") == "paid", F.col("pay_date").cast("timestamp"))
                                      .otherwise(F.col("invoice_date").cast("timestamp")))
          .cache())
inv.count()

def invoices_extract(d_str, incremental):
    d = dlit(d_str)
    if incremental:   # rows changed after E1: new invoices plus status changes
        r = inv.filter(F.col("modified_at") > dlit(E1).cast("timestamp"))
        return r.select("invoice_id", "subscription_id", "account_id", "invoice_date", "period_start", "period_end",
                        F.col("final_status").alias("status"), "total_amount", F.lit("USD").alias("currency"), "modified_at", "lines")
    r = inv.filter(F.col("invoice_date") <= d)           # state as of E1: invoices paid after E1 were still 'open'
    paid_later = F.col("modified_at") > d.cast("timestamp")
    return r.select("invoice_id", "subscription_id", "account_id", "invoice_date", "period_start", "period_end",
                    F.when(paid_later, "open").otherwise(F.col("final_status")).alias("status"), "total_amount",
                    F.lit("USD").alias("currency"),
                    F.when(paid_later, F.col("invoice_date").cast("timestamp")).otherwise(F.col("modified_at")).alias("modified_at"), "lines")

def with_dups(df, frac=0.01, seed=777):   # re-sent duplicates in the incremental extract
    return df.unionByName(df.sample(frac, seed=seed))

write(sub_asof(E1), f"billing/subscriptions/extract_date={E1}", fmt="json")
write(with_dups(sub_asof(HIST_END).filter(F.col("modified_at") > dlit(E1).cast("timestamp"))), f"billing/subscriptions/extract_date={HIST_END}", fmt="json")
write(invoices_extract(E1, False), f"billing/invoices/extract_date={E1}", fmt="json")
write(with_dups(invoices_extract(HIST_END, True)), f"billing/invoices/extract_date={HIST_END}", fmt="json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 6: PRODUCT TELEMETRY: usage events only while an account exists/active, proportional to seats
w = acct.select("account_id", "signup_date", "is_paid", "has_ai", "seats", "seats_after", "sub_start", "change_date",
                F.coalesce("end_date", END).alias("w_end"))
w = w.withColumn("active_days", F.greatest(F.lit(1), F.datediff(F.col("w_end"), F.col("signup_date")) + 1))
seats_eff = F.when(F.col("is_paid"), F.when(F.col("change_date").isNotNull(), (F.col("seats") + F.col("seats_after")) / 2.0).otherwise(F.col("seats").cast("double"))).otherwise(F.lit(1.5))
w = w.withColumn("seats_eff", seats_eff).withColumn("weight", F.col("seats_eff") * F.col("active_days") * F.when(F.col("is_paid"), 1.0).otherwise(0.3))
tot_w = w.agg(F.sum("weight")).first()[0]
rate = N_USAGE_EVENTS / (tot_w * 0.85)          # 0.85 = weekend thinning below
ev = (w.select("account_id", "signup_date", "active_days", "seats_eff", "has_ai",
               F.floor(F.col("weight") * rate + F.rand(sd())).cast("int").alias("n"))
        .filter("n >= 1")
        .select("*", F.explode(F.sequence(F.lit(1), F.col("n"))).alias("k"))
        .withColumn("ingest_date", F.date_add(F.col("signup_date"), F.floor(F.rand(sd()) * F.col("active_days")).cast("int")))
        .filter(~F.dayofweek("ingest_date").isin(1, 7) | (F.rand(sd()) < 0.4))      # weekdays busier than weekends
        .select(pid("E", F.monotonically_increasing_id(), 14).alias("event_id"),
                F.when(F.rand(sd()) < 0.005, F.lit(None)).otherwise(F.col("account_id")).alias("account_id"),   # PLANTED: missing account
                F.concat(F.col("account_id"), F.lit("-U"), F.lpad(F.floor(F.rand(sd()) * F.greatest(F.lit(1), F.round(F.col("seats_eff") * 0.6))).cast("string"), 3, "0")).alias("user_id"),
                F.when(F.col("has_ai"), pickw([("query_run", .35), ("dashboard_view", .28), ("export", .07), ("api_call", .12), ("ai_prompt", .18)], sd()))
                 .otherwise(pickw([("query_run", .42), ("dashboard_view", .36), ("export", .09), ("api_call", .13)], sd())).alias("event_type"),
                F.when(F.rand(sd()) < 0.002, F.lit(-1)).otherwise((F.rand(sd()) * 50 + 1).cast("int")).alias("quantity"),   # PLANTED: invalid qty
                "ingest_date", (F.rand(sd()) * 86400).cast("int").alias("sec"))
        .withColumn("lag_days", F.when(F.rand(sd()) < 0.02, (F.rand(sd()) * 5 + 1).cast("int")).otherwise(0))     # PLANTED: late events
        .withColumn("event_ts", F.col("ingest_date").cast("timestamp") + (F.col("sec") - F.col("lag_days") * 86400) * F.expr("INTERVAL 1 SECOND"))
        .drop("sec", "lag_days"))
ev = ev.cache()
ev = ev.unionByName(ev.sample(0.01, seed=SEED + 80))      # PLANTED: 1% delivered twice (at-least-once)
write(ev, "telemetry/usage_events_history", fmt="parquet", part="ingest_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VERIFY: read back what was written + business sanity checks
checks = {"marketing/campaigns": "csv", "marketing/ad_spend": "csv", "marketing/web_sessions": "csv",
          "crm/accounts": "csv", "crm/contacts": "csv", "crm/opportunities": "csv",
          "billing/subscriptions": "json", "billing/invoices": "json", "telemetry/usage_events_history": "parquet"}
for p, fmt in checks.items():
    r = spark.read.format(fmt).option("header", True).load(f"{BASE}/{p}")
    print(f"{p:40s} rows={r.count():>10,}")
ir = spark.read.json(f"{BASE}/billing/invoices/extract_date={HIST_END}")
print("billing lines (exploded, final extract):", ir.select(F.explode("lines")).count())
t = spark.read.parquet(f"{BASE}/telemetry/usage_events_history")
print("telemetry duplicate event_ids :", t.count() - t.select("event_id").distinct().count())
print("telemetry null account_id     :", t.filter("account_id is null").count())
print("telemetry negative quantity   :", t.filter("quantity < 0").count())
print("telemetry late events         :", t.filter(F.col("event_ts") < F.col("ingest_date").cast("timestamp")).count())
mrr = (subs.filter(F.col("end_date").isNull())
       .select((F.when(F.col("change_date").isNotNull(), F.col("seats_after")).otherwise(F.col("seats")) *
                by(F.when(F.col("change_date").isNotNull(), F.col("plan_after")).otherwise(F.col("plan")), PRICE_M)).alias("m"))
       .agg(F.sum("m")).first()[0]) or 0
print(f"active subscriptions: {subs.filter(F.col('end_date').isNull()).count():,} | approx platform MRR at end: ${mrr:,.0f} | ARR ~ ${mrr*12:,.0f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
