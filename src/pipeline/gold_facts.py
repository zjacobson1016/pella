# =============================================================================
# GOLD LAYER — Fact Materialized Views
#
# Materialized views join across the silver layer and dim tables to produce
# the two core analytical assets for the Pella parts forecasting use case.
#
# fact_demand_fulfillment  — Demand Signal → PO → Receiver → Invoice
# fact_work_order_completion — Work Order → Quote → Expected Completion Date
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# fact_demand_fulfillment
# The complete demand-to-payment chain for supply chain analytics.
#
# Key metrics:
#   forecast_variance     — accuracy of the demand signal
#   receipt_variance_days — delivery on time vs expected
#   invoice_amount        — total spend per part/supplier
#   days_to_payment       — supplier payment performance
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="fact_demand_fulfillment",
    cluster_by=["part_id", "po_date"],
    comment=(
        "Demand-to-payment fact. Links demand forecast → PO → receipt → invoice "
        "for supply chain analytics."
    ),
    table_properties={"quality": "gold", "layer": "fact"},
)
def fact_demand_fulfillment():
    po  = spark.table("silver_purchase_orders").alias("po")
    ds  = spark.table("silver_demand_signals").alias("ds")
    r   = spark.table("silver_receivers").alias("r")
    inv = spark.table("silver_invoices").alias("inv")
    dp1 = spark.table("dim_part_type1").alias("dp")

    return (
        po
        .join(ds,  F.col("ds.signal_id")   == F.col("po.signal_id"),  "left")
        .join(r,   F.col("r.po_id")         == F.col("po.po_id"),      "left")
        .join(inv, F.col("inv.po_id")        == F.col("po.po_id"),      "left")
        .join(dp1, F.col("dp.part_id")       == F.col("po.part_id"),    "left")
        .select(
            # Keys
            F.col("ds.signal_id"),
            F.col("po.po_id"),
            F.col("r.receiver_id"),
            F.col("inv.invoice_id"),
            F.col("po.part_id"),
            F.col("po.supplier_id"),
            # Demand layer
            F.col("ds.forecast_date"),
            F.col("ds.forecasted_qty"),
            F.col("ds.actual_qty"),
            F.col("ds.forecast_variance"),
            F.col("ds.signal_type"),
            F.col("ds.confidence_score"),
            F.col("ds.region").alias("demand_region"),
            # Purchase order layer
            F.col("po.po_date"),
            F.col("po.expected_receipt_date"),
            F.col("po.ordered_qty"),
            F.col("po.unit_price"),
            F.col("po.po_total_value"),
            F.col("po.po_status"),
            # Receiver layer
            F.col("r.received_date"),
            F.col("r.received_qty"),
            F.col("r.warehouse_location"),
            F.col("r.quality_status"),
            F.datediff(F.col("r.received_date"), F.col("po.expected_receipt_date"))
             .alias("receipt_variance_days"),
            # Invoice / payment layer
            F.col("inv.invoice_date"),
            F.col("inv.invoice_amount"),
            F.col("inv.payment_status"),
            F.col("inv.payment_date"),
            F.col("inv.days_to_payment"),
            # Part dimension (current state via Type 1)
            F.col("dp.part_name"),
            F.col("dp.category"),
            F.col("dp.subcategory"),
            F.col("dp.lead_time_days"),
        )
    )


# ---------------------------------------------------------------------------
# fact_work_order_completion
# Field work order lifecycle: Work Order → Customer Quote → Completion Date
#
# Expected completion date is governed by THREE constraints:
#   1. availability_constraint — part inventory / lead time
#   2. labor_constraint        — technician availability
#   3. piece_part_price        — drives total_amount and customer approval lag
#
# Key metrics:
#   days_to_completion    — quote date → expected completion date
#   total_amount          — full quote value (parts + labor)
#   remaining_hours       — technician capacity on scheduled date
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="fact_work_order_completion",
    cluster_by=["customer_id", "wo_id"],
    comment=(
        "Work order to customer quote fact. Completion date governed by "
        "parts availability, piece part price, and labor schedule."
    ),
    table_properties={"quality": "gold", "layer": "fact"},
)
def fact_work_order_completion():
    wo  = spark.table("silver_work_orders").alias("wo")
    q   = spark.table("silver_customer_quotes").alias("q")
    ls  = spark.table("silver_labor_schedules").alias("ls")
    dp1 = spark.table("dim_part_type1").alias("dp")
    dc  = spark.table("dim_customer_type1").alias("dc")

    return (
        wo
        .join(q,
              F.col("q.wo_id") == F.col("wo.wo_id"),
              "left")
        .join(ls,
              (F.col("ls.technician_id") == F.col("wo.technician_id"))
              & (F.col("ls.schedule_date") == F.col("wo.scheduled_date")),
              "left")
        .join(dp1,
              F.col("dp.part_id") == F.col("wo.part_id"),
              "left")
        .join(dc,
              F.col("dc.customer_id") == F.col("wo.customer_id"),
              "left")
        .select(
            # Keys
            F.col("wo.wo_id"),
            F.col("q.quote_id"),
            F.col("wo.customer_id"),
            F.col("wo.part_id"),
            F.col("wo.technician_id"),
            # Work order attributes
            F.col("wo.created_date"),
            F.col("wo.scheduled_date"),
            F.col("wo.wo_type"),
            F.col("wo.priority"),
            F.col("wo.wo_status"),
            F.col("wo.region"),
            # Quote / completion attributes
            F.col("q.quote_date"),
            F.col("q.parts_cost"),
            F.col("q.labor_cost"),
            F.col("q.total_amount"),
            F.col("q.expected_completion_date"),
            F.col("q.days_to_completion"),
            F.col("q.quote_status"),
            # Completion date constraints
            F.col("q.availability_constraint"),
            F.col("q.labor_constraint"),
            # Labor capacity context
            F.col("ls.available_hours"),
            F.col("ls.booked_hours"),
            F.col("ls.remaining_hours"),
            F.col("ls.skill_level"),
            # Part dimension (current price via Type 1)
            F.col("dp.part_name"),
            F.col("dp.category"),
            F.col("dp.piece_part_price").alias("current_piece_part_price"),
            F.col("dp.lead_time_days"),
            # Customer dimension (current state via Type 1)
            F.col("dc.customer_name"),
            F.col("dc.customer_tier"),
            F.col("dc.region").alias("customer_region"),
            F.col("dc.annual_revenue"),
        )
    )
