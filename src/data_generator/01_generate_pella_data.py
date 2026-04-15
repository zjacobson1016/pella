"""
Pella Parts Forecasting — Synthetic Data Generator
====================================================
Generates two batches of Parquet files written to Unity Catalog volumes:

  Batch 1 — Initial load (all INSERTs):
    /Volumes/mfg_mc_se_sa/pella/raw_data/parts_cdc/batch=1/
    /Volumes/mfg_mc_se_sa/pella/raw_data/customers_cdc/batch=1/
    /Volumes/mfg_mc_se_sa/pella/raw_data/demand_signals/
    /Volumes/mfg_mc_se_sa/pella/raw_data/purchase_orders/
    /Volumes/mfg_mc_se_sa/pella/raw_data/receivers/
    /Volumes/mfg_mc_se_sa/pella/raw_data/invoices/
    /Volumes/mfg_mc_se_sa/pella/raw_data/work_orders/
    /Volumes/mfg_mc_se_sa/pella/raw_data/customer_quotes/
    /Volumes/mfg_mc_se_sa/pella/raw_data/labor_schedules/

  Batch 2 — CDC update events (UPDATEs to demo SCD Type 1 vs Type 2):
    /Volumes/mfg_mc_se_sa/pella/raw_data/parts_cdc/batch=2/
    /Volumes/mfg_mc_se_sa/pella/raw_data/customers_cdc/batch=2/

Usage:
  pip install "databricks-connect>=16.4,<17.4" faker numpy pandas
  python 01_generate_pella_data.py [--batch {1,2,all}]

SCD Demo:
  Run pipeline after batch=1, then add batch=2 and run again.
  - dim_part_type1: piece_part_price shows ONLY latest value (Type 1 overwrites)
  - dim_part_type2: shows BOTH original AND updated price rows (__END_AT = NULL is current)
"""

import argparse
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
from databricks.connect import DatabricksSession, DatabricksEnv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CATALOG   = "mfg_mc_se_sa"
SCHEMA    = "pella"
VOLUME    = "raw_data"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

PROFILE = "group-demo"

N_PARTS      = 500
N_CUSTOMERS  = 1_000
N_SIGNALS    = 100_000
N_PO         = 15_000
N_RECEIVERS  = 14_000
N_INVOICES   = 13_000
N_WORK_ORDERS = 20_000
N_QUOTES     = 18_000
N_LABOR      = 8_000

# Records to UPDATE in batch 2 (subset of existing keys)
N_PART_UPDATES     = 80   # parts getting price changes
N_CUSTOMER_UPDATES = 120  # customers getting tier/address changes

END_DATE   = datetime(2025, 12, 31)
START_DATE = datetime(2024, 7, 1)

PART_CATEGORIES = ["WINDOW_HARDWARE", "DOOR_HARDWARE", "GLAZING", "WEATHERSTRIP",
                   "FRAME_COMPONENT", "SCREEN", "OPERATOR", "LOCK_SET", "HINGE", "SEALANT"]
PART_SUBCATS = {
    "WINDOW_HARDWARE": ["CASEMENT", "DOUBLE_HUNG", "AWNING", "SLIDING"],
    "DOOR_HARDWARE":   ["ENTRY", "PATIO", "STORM", "FRENCH"],
    "GLAZING":         ["LOW_E", "TEMPERED", "LAMINATED", "TRIPLE_PANE"],
    "WEATHERSTRIP":    ["FOAM", "RUBBER", "VINYL", "SILICONE"],
    "FRAME_COMPONENT": ["HEAD", "SILL", "JAMB", "MULLION"],
    "SCREEN":          ["FIBERGLASS", "ALUMINUM", "PET_PROOF", "SOLAR"],
    "OPERATOR":        ["CASEMENT_OP", "AWNING_OP", "FOLDING_OP"],
    "LOCK_SET":        ["SINGLE_POINT", "MULTI_POINT", "DEADBOLT"],
    "HINGE":           ["BUTT", "CONCEALED", "CONTINUOUS"],
    "SEALANT":         ["SILICONE", "POLYURETHANE", "LATEX"],
}
SUPPLIERS     = [f"SUPP-{i:03d}" for i in range(1, 31)]
REGIONS       = ["MIDWEST", "NORTHEAST", "SOUTHEAST", "SOUTHWEST", "WEST"]
CUSTOMER_TIERS = ["STANDARD", "PREFERRED", "PREMIUM", "ENTERPRISE"]
WO_TYPES      = ["INSTALLATION", "REPAIR", "REPLACEMENT", "WARRANTY", "INSPECTION"]
PRIORITIES    = ["LOW", "MEDIUM", "HIGH", "URGENT"]
CONSTRAINTS   = ["PARTS_ON_ORDER", "IN_STOCK", "LEAD_TIME_14D", "LEAD_TIME_7D", "SPECIAL_ORDER"]
LABOR_CONSTRAINTS = ["AVAILABLE", "PARTIAL_AVAILABILITY", "FULLY_BOOKED", "NEXT_WEEK"]
SKILL_LEVELS  = ["APPRENTICE", "JOURNEYMAN", "SENIOR", "MASTER"]


# ---------------------------------------------------------------------------
# Spark Setup
# ---------------------------------------------------------------------------
def get_spark():
    env = DatabricksEnv().withDependencies("faker", "numpy", "pandas")
    spark = (DatabricksSession.builder
             .profile(PROFILE)
             .withEnvironment(env)
             .serverless(True)
             .getOrCreate())
    return spark


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
def create_infra(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    print(f"[infra] Schema and volume ready: {CATALOG}.{SCHEMA} / {VOLUME}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def rand_date(start: datetime, end: datetime) -> date:
    delta = (end - start).days
    return (start + timedelta(days=int(np.random.randint(0, delta)))).date()


def lognormal_price(mu=4.5, sigma=1.0, low=5.0, high=2500.0) -> float:
    v = np.random.lognormal(mu, sigma)
    return float(np.clip(v, low, high))


# ---------------------------------------------------------------------------
# BATCH 1 — Parts CDC (all INSERT)
# ---------------------------------------------------------------------------
def generate_parts_batch1() -> pd.DataFrame:
    np.random.seed(42)

    rows = []
    now_ts = END_DATE - timedelta(days=180)

    for i in range(N_PARTS):
        cat = PART_CATEGORIES[i % len(PART_CATEGORIES)]
        sub = PART_SUBCATS[cat][i % len(PART_SUBCATS[cat])]
        rows.append({
            "part_id":          f"PART-{i:05d}",
            "part_number":      f"PW-{cat[:3]}-{i:05d}",
            "part_name":        f"{cat.replace('_', ' ').title()} {sub.replace('_', ' ').title()} #{i}",
            "category":         cat,
            "subcategory":      sub,
            "piece_part_price": round(lognormal_price(), 2),
            "standard_cost":    round(lognormal_price(mu=4.0, sigma=0.8), 2),
            "lead_time_days":   int(np.random.choice([7, 14, 21, 30, 45, 60],
                                                     p=[0.15, 0.30, 0.25, 0.15, 0.10, 0.05])),
            "supplier_id":      SUPPLIERS[i % len(SUPPLIERS)],
            "unit_of_measure":  np.random.choice(["EA", "EA", "EA", "PK", "FT"], p=[0.7, 0.1, 0.1, 0.05, 0.05]),
            "is_active":        True,
            "_op":              "INSERT",
            "_sequence":        i + 1,
            "updated_at":       now_ts - timedelta(days=np.random.randint(1, 180)),
        })

    return pd.DataFrame(rows)


def generate_parts_batch2(parts_df: pd.DataFrame) -> pd.DataFrame:
    """Price change events — subset of parts get an UPDATE with new piece_part_price."""
    np.random.seed(99)

    idx = np.random.choice(len(parts_df), size=N_PART_UPDATES, replace=False)
    update_rows = []
    for rank, i in enumerate(idx):
        row = parts_df.iloc[i].to_dict()
        old_price = row["piece_part_price"]
        # simulate supplier price increase (20-40%) or decrease (5-15%)
        if np.random.random() < 0.7:
            new_price = round(old_price * np.random.uniform(1.20, 1.40), 2)
        else:
            new_price = round(old_price * np.random.uniform(0.85, 0.95), 2)
        row["piece_part_price"] = new_price
        row["_op"]              = "UPDATE"
        row["_sequence"]        = N_PARTS + rank + 1   # higher sequence = later event
        row["updated_at"]       = END_DATE - timedelta(days=np.random.randint(1, 30))
        update_rows.append(row)

    return pd.DataFrame(update_rows)


# ---------------------------------------------------------------------------
# BATCH 1 — Customers CDC (all INSERT)
# ---------------------------------------------------------------------------
def generate_customers_batch1() -> pd.DataFrame:
    np.random.seed(43)

    rows = []
    streets = ["Main St", "Oak Ave", "Elm Dr", "Maple Ln", "Cedar Blvd",
               "Pine Rd", "Birch Ct", "Walnut Way", "Spruce Pl", "Ash Terrace"]
    cities_by_region = {
        "MIDWEST":   ["Des Moines", "Indianapolis", "Columbus", "Milwaukee", "Kansas City"],
        "NORTHEAST": ["Philadelphia", "Boston", "Providence", "Albany", "Hartford"],
        "SOUTHEAST": ["Atlanta", "Charlotte", "Nashville", "Tampa", "Raleigh"],
        "SOUTHWEST": ["Phoenix", "Albuquerque", "Tucson", "El Paso", "Las Vegas"],
        "WEST":      ["Seattle", "Portland", "Denver", "Salt Lake City", "Sacramento"],
    }
    state_by_region = {
        "MIDWEST": "IA", "NORTHEAST": "PA", "SOUTHEAST": "GA",
        "SOUTHWEST": "AZ", "WEST": "WA"
    }

    for i in range(N_CUSTOMERS):
        region = REGIONS[i % len(REGIONS)]
        tier   = np.random.choice(CUSTOMER_TIERS, p=[0.45, 0.30, 0.18, 0.07])
        city   = cities_by_region[region][i % len(cities_by_region[region])]
        rev_mu = {"STANDARD": 5.5, "PREFERRED": 6.5, "PREMIUM": 7.2, "ENTERPRISE": 8.0}[tier]
        rows.append({
            "customer_id":      f"CUST-{i:05d}",
            "customer_name":    f"Pella Customer {i:05d} LLC",
            "address":          f"{np.random.randint(100, 9999)} {streets[i % len(streets)]}",
            "city":             city,
            "state":            state_by_region[region],
            "zip":              f"{np.random.randint(10000, 99999):05d}",
            "customer_tier":    tier,
            "account_manager":  f"AM-{(i % 20):03d}",
            "region":           region,
            "annual_revenue":   round(np.exp(np.random.normal(rev_mu, 0.8)), 2),
            "is_active":        True,
            "_op":              "INSERT",
            "_sequence":        i + 1,
            "updated_at":       END_DATE - timedelta(days=np.random.randint(1, 365)),
        })

    return pd.DataFrame(rows)


def generate_customers_batch2(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Tier upgrades and address changes — subset of customers."""
    np.random.seed(100)

    tier_upgrade = {"STANDARD": "PREFERRED", "PREFERRED": "PREMIUM",
                    "PREMIUM": "ENTERPRISE", "ENTERPRISE": "ENTERPRISE"}
    idx = np.random.choice(len(customers_df), size=N_CUSTOMER_UPDATES, replace=False)
    update_rows = []
    for rank, i in enumerate(idx):
        row = customers_df.iloc[i].to_dict()
        change_type = np.random.choice(["tier_upgrade", "address_change"], p=[0.55, 0.45])
        if change_type == "tier_upgrade":
            row["customer_tier"] = tier_upgrade[row["customer_tier"]]
        else:
            row["address"] = f"{np.random.randint(100, 9999)} New Address Blvd"
            row["zip"]     = f"{np.random.randint(10000, 99999):05d}"
        row["_op"]       = "UPDATE"
        row["_sequence"] = N_CUSTOMERS + rank + 1
        row["updated_at"] = END_DATE - timedelta(days=np.random.randint(1, 30))
        update_rows.append(row)

    return pd.DataFrame(update_rows)


# ---------------------------------------------------------------------------
# Transactional Tables
# ---------------------------------------------------------------------------
def generate_demand_signals(parts_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(44)

    part_ids = parts_df["part_id"].tolist()
    rows = []
    for i in range(N_SIGNALS):
        part_id = part_ids[i % len(part_ids)]
        region  = REGIONS[i % len(REGIONS)]
        fcast_date = rand_date(START_DATE, END_DATE)
        forecasted = int(np.random.lognormal(3.0, 0.8))
        forecasted = max(1, min(forecasted, 500))
        actual     = max(0, int(forecasted * np.random.normal(1.0, 0.15)))
        rows.append({
            "signal_id":       f"SIG-{i:07d}",
            "part_id":         part_id,
            "forecast_date":   fcast_date,
            "forecasted_qty":  forecasted,
            "actual_qty":      actual,
            "signal_type":     np.random.choice(
                ["SEASONAL", "TRENDING", "SPIKE", "BASELINE"],
                p=[0.35, 0.25, 0.15, 0.25]
            ),
            "confidence_score": round(np.random.beta(8, 2), 4),
            "region":           region,
            "created_at":       END_DATE - timedelta(days=np.random.randint(1, 365)),
        })
    return pd.DataFrame(rows)


def generate_purchase_orders(signals_df: pd.DataFrame,
                             parts_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(45)
    price_map = dict(zip(parts_df["part_id"], parts_df["piece_part_price"]))
    sig_ids   = signals_df["signal_id"].tolist()
    part_ids  = signals_df["part_id"].tolist()

    rows = []
    for i in range(N_PO):
        idx         = i % len(sig_ids)
        part_id     = part_ids[idx]
        po_date_val = rand_date(START_DATE, END_DATE - timedelta(days=30))
        lead        = int(parts_df[parts_df["part_id"] == part_id]["lead_time_days"].values[0]) \
                      if part_id in price_map else 14
        exp_rcpt    = po_date_val + timedelta(days=lead)
        unit_price  = round(price_map.get(part_id, 50.0) * np.random.uniform(0.90, 1.10), 2)
        rows.append({
            "po_id":                f"PO-{i:07d}",
            "signal_id":            sig_ids[idx],
            "part_id":              part_id,
            "supplier_id":          SUPPLIERS[i % len(SUPPLIERS)],
            "po_date":              po_date_val,
            "expected_receipt_date": exp_rcpt,
            "ordered_qty":          max(1, int(np.random.lognormal(2.5, 0.7))),
            "unit_price":           unit_price,
            "po_status":            np.random.choice(
                ["OPEN", "ACKNOWLEDGED", "SHIPPED", "RECEIVED", "CLOSED", "CANCELLED"],
                p=[0.10, 0.15, 0.20, 0.25, 0.25, 0.05]
            ),
            "buyer_id":             f"BUYER-{(i % 15):03d}",
            "created_at":           END_DATE - timedelta(days=np.random.randint(1, 365)),
        })
    return pd.DataFrame(rows)


def generate_receivers(pos_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(46)
    warehouses = ["WH-DES-MOINES", "WH-STORM-LAKE", "WH-SIOUX-CITY",
                  "WH-CEDAR-RAPIDS", "WH-DALLAS"]

    po_sample = pos_df.sample(n=N_RECEIVERS, replace=True, random_state=46)
    rows = []
    for i, (_, po) in enumerate(po_sample.iterrows()):
        exp_date = po["expected_receipt_date"]
        if isinstance(exp_date, str):
            exp_date = datetime.strptime(exp_date, "%Y-%m-%d").date()
        delay_days = int(np.random.choice([-3, -1, 0, 0, 1, 3, 7, 14],
                                          p=[0.05, 0.10, 0.20, 0.20, 0.20, 0.15, 0.07, 0.03]))
        recv_date  = exp_date + timedelta(days=delay_days)
        rows.append({
            "receiver_id":      f"RCV-{i:07d}",
            "po_id":            po["po_id"],
            "part_id":          po["part_id"],
            "received_date":    recv_date,
            "received_qty":     max(1, int(po["ordered_qty"] * np.random.uniform(0.90, 1.05))),
            "warehouse_location": warehouses[i % len(warehouses)],
            "quality_status":   np.random.choice(
                ["ACCEPTED", "ACCEPTED", "ACCEPTED", "PARTIAL_ACCEPT", "REJECTED"],
                p=[0.70, 0.10, 0.10, 0.07, 0.03]
            ),
            "inspector_id":     f"INSP-{(i % 10):03d}",
            "created_at":       END_DATE - timedelta(days=np.random.randint(1, 300)),
        })
    return pd.DataFrame(rows)


def generate_invoices(pos_df: pd.DataFrame, rcv_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(47)

    rcv_sample = rcv_df.sample(n=N_INVOICES, replace=True, random_state=47)
    po_map     = dict(zip(pos_df["po_id"], pos_df["unit_price"]))
    rows = []
    for i, (_, rcv) in enumerate(rcv_sample.iterrows()):
        inv_date = rcv["received_date"]
        if isinstance(inv_date, str):
            inv_date = datetime.strptime(inv_date, "%Y-%m-%d").date()
        inv_date = inv_date + timedelta(days=int(np.random.randint(0, 7)))
        unit_p   = po_map.get(rcv["po_id"], 50.0)
        amount   = round(unit_p * rcv["received_qty"] * np.random.uniform(0.98, 1.02), 2)
        d2pay    = int(np.random.choice([15, 30, 45, 60, 90], p=[0.10, 0.50, 0.25, 0.10, 0.05]))
        paid     = np.random.random() < 0.75
        rows.append({
            "invoice_id":     f"INV-{i:07d}",
            "po_id":          rcv["po_id"],
            "receiver_id":    rcv["receiver_id"],
            "supplier_id":    SUPPLIERS[i % len(SUPPLIERS)],
            "invoice_date":   inv_date,
            "invoice_amount": amount,
            "payment_status": "PAID" if paid else np.random.choice(["PENDING", "OVERDUE"], p=[0.7, 0.3]),
            "payment_date":   (inv_date + timedelta(days=d2pay)) if paid else None,
            "days_to_payment": d2pay if paid else None,
            "created_at":     END_DATE - timedelta(days=np.random.randint(1, 300)),
        })
    return pd.DataFrame(rows)


def generate_work_orders(customers_df: pd.DataFrame,
                         parts_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(48)
    cust_ids = customers_df["customer_id"].tolist()
    part_ids = parts_df["part_id"].tolist()
    techs    = [f"TECH-{i:04d}" for i in range(50)]

    rows = []
    for i in range(N_WORK_ORDERS):
        created  = rand_date(START_DATE, END_DATE - timedelta(days=14))
        priority = np.random.choice(PRIORITIES, p=[0.25, 0.45, 0.20, 0.10])
        sched_offset = {"LOW": 21, "MEDIUM": 14, "HIGH": 7, "URGENT": 2}[priority]
        sched_date   = created + timedelta(days=int(np.random.randint(1, sched_offset + 1)))
        rows.append({
            "wo_id":          f"WO-{i:07d}",
            "customer_id":    cust_ids[i % len(cust_ids)],
            "part_id":        part_ids[i % len(part_ids)],
            "technician_id":  techs[i % len(techs)],
            "created_date":   created,
            "scheduled_date": sched_date,
            "wo_type":        np.random.choice(WO_TYPES, p=[0.35, 0.30, 0.20, 0.10, 0.05]),
            "priority":       priority,
            "wo_status":      np.random.choice(
                ["OPEN", "SCHEDULED", "IN_PROGRESS", "COMPLETED", "ON_HOLD", "CANCELLED"],
                p=[0.15, 0.25, 0.20, 0.30, 0.07, 0.03]
            ),
            "description":    f"Pella service work order #{i} - {WO_TYPES[i % len(WO_TYPES)]}",
            "region":         REGIONS[i % len(REGIONS)],
            "created_at":     END_DATE - timedelta(days=np.random.randint(1, 365)),
        })
    return pd.DataFrame(rows)


def generate_customer_quotes(wo_df: pd.DataFrame,
                             parts_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(49)
    price_map   = dict(zip(parts_df["part_id"], parts_df["piece_part_price"]))
    lead_map    = dict(zip(parts_df["part_id"], parts_df["lead_time_days"]))
    wo_sample   = wo_df.sample(n=N_QUOTES, replace=True, random_state=49)

    rows = []
    for i, (_, wo) in enumerate(wo_sample.iterrows()):
        part_id      = wo["part_id"]
        quote_date   = wo["created_date"]
        if isinstance(quote_date, str):
            quote_date = datetime.strptime(quote_date, "%Y-%m-%d").date()
        quote_date   = quote_date + timedelta(days=int(np.random.randint(0, 3)))

        avail        = np.random.choice(CONSTRAINTS, p=[0.25, 0.30, 0.20, 0.15, 0.10])
        labor_c      = np.random.choice(LABOR_CONSTRAINTS, p=[0.40, 0.30, 0.15, 0.15])
        lead         = lead_map.get(part_id, 14)

        # Completion date driven by the binding constraint
        parts_delay  = {"PARTS_ON_ORDER": lead, "IN_STOCK": 0, "LEAD_TIME_14D": 14,
                        "LEAD_TIME_7D": 7, "SPECIAL_ORDER": 45}[avail]
        labor_delay  = {"AVAILABLE": 0, "PARTIAL_AVAILABILITY": 3,
                        "FULLY_BOOKED": 7, "NEXT_WEEK": 7}[labor_c]
        total_delay  = max(parts_delay, labor_delay) + int(np.random.randint(1, 5))

        parts_cost   = round(price_map.get(part_id, 50.0) * np.random.uniform(1.0, 3.0), 2)
        labor_cost   = round(np.random.lognormal(4.5, 0.5), 2)
        rows.append({
            "quote_id":                 f"QT-{i:07d}",
            "wo_id":                    wo["wo_id"],
            "customer_id":              wo["customer_id"],
            "part_id":                  part_id,
            "quote_date":               quote_date,
            "parts_cost":               parts_cost,
            "labor_cost":               labor_cost,
            "total_amount":             round(parts_cost + labor_cost, 2),
            "expected_completion_date": quote_date + timedelta(days=total_delay),
            "availability_constraint":  avail,
            "labor_constraint":         labor_c,
            "quote_status":             np.random.choice(
                ["DRAFT", "SENT", "ACCEPTED", "DECLINED", "EXPIRED"],
                p=[0.10, 0.25, 0.45, 0.10, 0.10]
            ),
            "created_at":               END_DATE - timedelta(days=np.random.randint(1, 365)),
        })
    return pd.DataFrame(rows)


def generate_labor_schedules() -> pd.DataFrame:
    np.random.seed(50)
    techs = [f"TECH-{i:04d}" for i in range(50)]

    rows = []
    count = 0
    for tech_id in techs:
        region = REGIONS[int(tech_id.split("-")[1]) % len(REGIONS)]
        skill  = SKILL_LEVELS[int(tech_id.split("-")[1]) % len(SKILL_LEVELS)]
        for _ in range(N_LABOR // len(techs)):
            sched_date    = rand_date(START_DATE, END_DATE)
            avail_hours   = round(np.random.choice([8.0, 8.0, 8.0, 6.0, 4.0], p=[0.5, 0.2, 0.1, 0.1, 0.1]), 1)
            booked_frac   = np.random.beta(2, 3)
            booked_hours  = round(avail_hours * booked_frac, 1)
            rows.append({
                "schedule_id":    f"SCHED-{count:07d}",
                "technician_id":  tech_id,
                "technician_name": f"Technician {tech_id}",
                "schedule_date":  sched_date,
                "available_hours": avail_hours,
                "booked_hours":   booked_hours,
                "region":         region,
                "skill_level":    skill,
                "created_at":     END_DATE - timedelta(days=np.random.randint(1, 365)),
            })
            count += 1
    return pd.DataFrame(rows[:N_LABOR])


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------
def write_parquet(spark, pdf: pd.DataFrame, path: str, n_partitions: int = 16):
    sdf = spark.createDataFrame(pdf)
    sdf.repartition(n_partitions).write.mode("overwrite").parquet(path)
    print(f"  -> {path}  ({len(pdf):,} rows)")


def write_parquet_batch(spark, pdf: pd.DataFrame, path: str, batch: int, n_partitions: int = 8):
    """Write to a sub-folder keyed by batch number (for AutoLoader incremental demo)."""
    sdf = spark.createDataFrame(pdf)
    sdf.repartition(n_partitions).write.mode("overwrite").parquet(f"{path}/batch={batch}")
    print(f"  -> {path}/batch={batch}  ({len(pdf):,} rows)")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def run_batch1(spark):
    print("\n=== BATCH 1: Initial load (INSERT events) ===")

    print("\n[1/9] Parts CDC batch 1...")
    parts_b1 = generate_parts_batch1()
    write_parquet_batch(spark, parts_b1, f"{BASE_PATH}/parts_cdc", batch=1)

    print("\n[2/9] Customers CDC batch 1...")
    cust_b1 = generate_customers_batch1()
    write_parquet_batch(spark, cust_b1, f"{BASE_PATH}/customers_cdc", batch=1)

    print("\n[3/9] Demand signals...")
    sig_df = generate_demand_signals(parts_b1)
    write_parquet(spark, sig_df, f"{BASE_PATH}/demand_signals", n_partitions=16)

    print("\n[4/9] Purchase orders...")
    po_df = generate_purchase_orders(sig_df, parts_b1)
    write_parquet(spark, po_df, f"{BASE_PATH}/purchase_orders", n_partitions=16)

    print("\n[5/9] Receivers...")
    rcv_df = generate_receivers(po_df)
    write_parquet(spark, rcv_df, f"{BASE_PATH}/receivers", n_partitions=16)

    print("\n[6/9] Invoices...")
    inv_df = generate_invoices(po_df, rcv_df)
    write_parquet(spark, inv_df, f"{BASE_PATH}/invoices", n_partitions=16)

    print("\n[7/9] Work orders...")
    wo_df = generate_work_orders(cust_b1, parts_b1)
    write_parquet(spark, wo_df, f"{BASE_PATH}/work_orders", n_partitions=16)

    print("\n[8/9] Customer quotes...")
    q_df = generate_customer_quotes(wo_df, parts_b1)
    write_parquet(spark, q_df, f"{BASE_PATH}/customer_quotes", n_partitions=16)

    print("\n[9/9] Labor schedules...")
    lab_df = generate_labor_schedules()
    write_parquet(spark, lab_df, f"{BASE_PATH}/labor_schedules", n_partitions=8)

    print("\n[BATCH 1 COMPLETE] Run the pipeline now to load the initial state.")
    print("  After pipeline completes:")
    print("  - dim_part_type1 and dim_part_type2 will have identical rows (only INSERTs so far)")
    print("  - All fact tables will be populated")
    return parts_b1, cust_b1


def run_batch2(spark):
    """
    Load batch1 DataFrames from volume to derive batch2 UPDATEs.
    Reads the parquet files written in batch1 to get original values.
    """
    print("\n=== BATCH 2: CDC update events (to demo SCD Type 1 vs Type 2) ===")

    print("\n[1/2] Loading batch1 parts from volume...")
    parts_b1 = spark.read.parquet(f"{BASE_PATH}/parts_cdc/batch=1").toPandas()
    parts_b2 = generate_parts_batch2(parts_b1)
    write_parquet_batch(spark, parts_b2, f"{BASE_PATH}/parts_cdc", batch=2)

    print("\n[2/2] Loading batch1 customers from volume...")
    cust_b1 = spark.read.parquet(f"{BASE_PATH}/customers_cdc/batch=1").toPandas()
    cust_b2 = generate_customers_batch2(cust_b1)
    write_parquet_batch(spark, cust_b2, f"{BASE_PATH}/customers_cdc", batch=2)

    print("\n[BATCH 2 COMPLETE] Re-run the pipeline to process CDC updates.")
    print("  After pipeline completes, compare SCD behavior:")
    print()
    print("  -- SCD TYPE 1 (overwrite): only latest price --")
    print("  SELECT part_id, part_name, piece_part_price")
    print("  FROM mfg_mc_se_sa.pella.dim_part_type1")
    print("  WHERE part_id = 'PART-00001';")
    print()
    print("  -- SCD TYPE 2 (history): see both old and new price --")
    print("  SELECT part_id, part_name, piece_part_price, __START_AT, __END_AT")
    print("  FROM mfg_mc_se_sa.pella.dim_part_type2")
    print("  WHERE part_id = 'PART-00001'")
    print("  ORDER BY __START_AT;")
    print()
    print("  -- Customer tier journey (Type 2) --")
    print("  SELECT customer_id, customer_name, customer_tier, address, __START_AT, __END_AT")
    print("  FROM mfg_mc_se_sa.pella.dim_customer_type2")
    print("  WHERE __END_AT IS NULL  -- current rows only")
    print("  LIMIT 20;")


def main():
    parser = argparse.ArgumentParser(description="Generate Pella synthetic data")
    parser.add_argument("--batch", choices=["1", "2", "all"], default="all",
                        help="Which batch to generate (default: all)")
    args = parser.parse_args()

    spark = get_spark()
    create_infra(spark)

    if args.batch in ("1", "all"):
        run_batch1(spark)

    if args.batch in ("2", "all"):
        run_batch2(spark)

    print("\nDone.")


if __name__ == "__main__":
    main()
