# =============================================================================
# GOLD LAYER — SCD Type 1 and Type 2 Dimensions via AUTO CDC
#
# AUTO CDC (dp.create_auto_cdc_flow) processes CDC events from silver tables
# and maintains two separate views of each entity:
#
#   Type 1 — Upsert/overwrite: always reflects the current state. No history.
#   Type 2 — Versioned rows: every change creates a new row with __START_AT /
#             __END_AT timestamps. Current row has __END_AT = NULL.
#
# SCD Demo Queries (after batch 2 pipeline run):
#
#   -- Type 1: one row per part (latest price only) --
#   SELECT part_id, piece_part_price
#   FROM mfg_mc_se_sa.pella.dim_part_type1
#   WHERE part_id = 'PART-00001';
#
#   -- Type 2: full price history per part --
#   SELECT part_id, piece_part_price, __START_AT, __END_AT
#   FROM mfg_mc_se_sa.pella.dim_part_type2
#   WHERE part_id = 'PART-00001'
#   ORDER BY __START_AT;
#
#   -- Type 2: customer tier journey (current rows only) --
#   SELECT customer_id, customer_name, customer_tier, __START_AT
#   FROM mfg_mc_se_sa.pella.dim_customer_type2
#   WHERE __END_AT IS NULL
#   ORDER BY customer_id;
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ---------------------------------------------------------------------------
# dim_part_type1 — SCD Type 1 (overwrite on change, no history)
#
# When piece_part_price or category changes, the old value is overwritten.
# After batch 2: shows ONLY the latest price for each part.
# ---------------------------------------------------------------------------
dp.create_streaming_table("dim_part_type1")

dp.create_auto_cdc_flow(
    target="dim_part_type1",
    source="silver_parts",
    keys=["part_id"],
    sequence_by=col("_sequence"),
    apply_as_deletes=expr("_op = 'DELETE'"),
    except_column_list=["_op", "_sequence"],
    stored_as_scd_type="1",
)


# ---------------------------------------------------------------------------
# dim_part_type2 — SCD Type 2 (versioned rows, full price/category history)
#
# Each change creates a new row. Previous row __END_AT = change timestamp.
# After batch 2: two rows per updated part — original price and new price.
# Query current rows: WHERE __END_AT IS NULL
# ---------------------------------------------------------------------------
dp.create_streaming_table("dim_part_type2")

dp.create_auto_cdc_flow(
    target="dim_part_type2",
    source="silver_parts",
    keys=["part_id"],
    sequence_by=col("_sequence"),
    apply_as_deletes=expr("_op = 'DELETE'"),
    except_column_list=["_op", "_sequence"],
    stored_as_scd_type=2,
)


# ---------------------------------------------------------------------------
# dim_customer_type1 — SCD Type 1 (current customer state only)
#
# Tier upgrades (e.g., STANDARD → PREMIUM) overwrite the old tier value.
# ---------------------------------------------------------------------------
dp.create_streaming_table("dim_customer_type1")

dp.create_auto_cdc_flow(
    target="dim_customer_type1",
    source="silver_customers",
    keys=["customer_id"],
    sequence_by=col("_sequence"),
    apply_as_deletes=expr("_op = 'DELETE'"),
    except_column_list=["_op", "_sequence"],
    stored_as_scd_type="1",
)


# ---------------------------------------------------------------------------
# dim_customer_type2 — SCD Type 2 (customer tier and address history)
#
# Captures every tier upgrade and address change as a versioned row.
# After batch 2: reveals the customer's tier journey over time.
# ---------------------------------------------------------------------------
dp.create_streaming_table("dim_customer_type2")

dp.create_auto_cdc_flow(
    target="dim_customer_type2",
    source="silver_customers",
    keys=["customer_id"],
    sequence_by=col("_sequence"),
    apply_as_deletes=expr("_op = 'DELETE'"),
    except_column_list=["_op", "_sequence"],
    stored_as_scd_type=2,
)
