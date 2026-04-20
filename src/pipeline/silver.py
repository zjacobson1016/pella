# =============================================================================
# SILVER LAYER — Cleaning, casting, and validation from bronze streaming tables test.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType


# ---------------------------------------------------------------------------
# Parts CDC (cleaned) — feeds SCD Type 1 and Type 2 gold dimensions
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_parts",
    cluster_by=["part_id"],
    comment="Cleaned and validated parts CDC events. Source for dim_part_type1 and dim_part_type2.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_part_id", "part_id IS NOT NULL")
@dp.expect_or_drop("valid_price", "piece_part_price >= 0")
@dp.expect_or_drop("valid_cdc_op", "_op IN ('INSERT', 'UPDATE', 'DELETE')")
@dp.expect("has_part_name", "part_name IS NOT NULL")
@dp.expect("has_category", "category IS NOT NULL")
@dp.expect("cost_not_exceeds_price", "standard_cost <= piece_part_price")
def silver_parts():
    return (
        spark.readStream.table("bronze_parts")
        .filter(
            F.col("part_id").isNotNull()
            & (F.col("piece_part_price") >= 0)
            & F.col("_op").isin("INSERT", "UPDATE", "DELETE")
        )
        .select(
            F.col("part_id"),
            F.col("part_number"),
            F.trim(F.col("part_name")).alias("part_name"),
            F.upper(F.trim(F.col("category"))).alias("category"),
            F.upper(F.trim(F.col("subcategory"))).alias("subcategory"),
            F.col("piece_part_price").cast(DecimalType(12, 2)).alias("piece_part_price"),
            F.col("standard_cost").cast(DecimalType(12, 2)).alias("standard_cost"),
            F.col("lead_time_days").cast(IntegerType()).alias("lead_time_days"),
            F.col("supplier_id"),
            F.coalesce(F.col("unit_of_measure"), F.lit("EA")).alias("unit_of_measure"),
            F.coalesce(F.col("is_active"), F.lit(True)).alias("is_active"),
            F.col("_op"),
            F.col("_sequence"),
            F.col("updated_at"),
        )
    )


# ---------------------------------------------------------------------------
# Customers CDC (cleaned) — feeds SCD Type 1 and Type 2 gold dimensions
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_customers",
    cluster_by=["customer_id"],
    comment="Cleaned and validated customer CDC events. Source for dim_customer_type1 and dim_customer_type2.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_cdc_op", "_op IN ('INSERT', 'UPDATE', 'DELETE')")
@dp.expect("has_customer_name", "customer_name IS NOT NULL")
@dp.expect("valid_tier", "customer_tier IN ('STANDARD', 'PREMIUM', 'ENTERPRISE', 'GOLD', 'SILVER', 'BRONZE')")
@dp.expect("valid_revenue", "annual_revenue >= 0")
@dp.expect("has_region", "region IS NOT NULL")
def silver_customers():
    return (
        spark.readStream.table("bronze_customers")
        .filter(
            F.col("customer_id").isNotNull()
            & F.col("_op").isin("INSERT", "UPDATE", "DELETE")
        )
        .select(
            F.col("customer_id"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.trim(F.col("address")).alias("address"),
            F.trim(F.col("city")).alias("city"),
            F.upper(F.trim(F.col("state"))).alias("state"),
            F.trim(F.col("zip")).alias("zip"),
            F.upper(F.trim(F.col("customer_tier"))).alias("customer_tier"),
            F.trim(F.col("account_manager")).alias("account_manager"),
            F.upper(F.trim(F.col("region"))).alias("region"),
            F.col("annual_revenue").cast(DecimalType(14, 2)).alias("annual_revenue"),
            F.coalesce(F.col("is_active"), F.lit(True)).alias("is_active"),
            F.col("_op"),
            F.col("_sequence"),
            F.col("updated_at"),
        )
    )


# ---------------------------------------------------------------------------
# Demand Signals (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_demand_signals",
    cluster_by=["part_id", "forecast_date"],
    comment="Cleaned demand signals. Drives purchase order creation.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_signal_id", "signal_id IS NOT NULL")
@dp.expect_or_fail("valid_part_id", "part_id IS NOT NULL")
@dp.expect_or_drop("positive_forecast", "forecasted_qty > 0")
@dp.expect("valid_confidence", "confidence_score BETWEEN 0 AND 1")
@dp.expect("has_forecast_date", "forecast_date IS NOT NULL")
def silver_demand_signals():
    return (
        spark.readStream.table("bronze_demand_signals")
        .filter(
            F.col("signal_id").isNotNull()
            & F.col("part_id").isNotNull()
            & (F.col("forecasted_qty") > 0)
        )
        .select(
            F.col("signal_id"),
            F.col("part_id"),
            F.col("forecast_date").cast(DateType()).alias("forecast_date"),
            F.col("forecasted_qty").cast(IntegerType()).alias("forecasted_qty"),
            F.col("actual_qty").cast(IntegerType()).alias("actual_qty"),
            (F.col("actual_qty") - F.col("forecasted_qty"))
            .cast(IntegerType())
            .alias("forecast_variance"),
            F.col("signal_type"),
            F.col("confidence_score").cast(DecimalType(5, 4)).alias("confidence_score"),
            F.col("region"),
            F.col("created_at"),
        )
    )


# ---------------------------------------------------------------------------
# Purchase Orders (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_purchase_orders",
    cluster_by=["po_id", "part_id"],
    comment="Cleaned purchase orders. Downstream of demand signals.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_po_id", "po_id IS NOT NULL")
@dp.expect_or_fail("valid_part_id", "part_id IS NOT NULL")
@dp.expect_or_drop("positive_qty", "ordered_qty > 0")
@dp.expect_or_drop("positive_unit_price", "unit_price > 0")
@dp.expect("receipt_after_order", "expected_receipt_date >= po_date")
@dp.expect("valid_po_status", "po_status IN ('OPEN', 'CLOSED', 'CANCELLED', 'PENDING', 'RECEIVED')")
def silver_purchase_orders():
    return (
        spark.readStream.table("bronze_purchase_orders")
        .filter(
            F.col("po_id").isNotNull()
            & F.col("part_id").isNotNull()
        )
        .select(
            F.col("po_id"),
            F.col("signal_id"),
            F.col("part_id"),
            F.col("supplier_id"),
            F.col("po_date").cast(DateType()).alias("po_date"),
            F.col("expected_receipt_date").cast(DateType()).alias("expected_receipt_date"),
            F.col("ordered_qty").cast(IntegerType()).alias("ordered_qty"),
            F.col("unit_price").cast(DecimalType(12, 2)).alias("unit_price"),
            (F.col("ordered_qty") * F.col("unit_price"))
            .cast(DecimalType(14, 2))
            .alias("po_total_value"),
            F.upper(F.col("po_status")).alias("po_status"),
            F.col("buyer_id"),
            F.col("created_at"),
        )
    )


# ---------------------------------------------------------------------------
# Receivers / Goods Receipts (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_receivers",
    cluster_by=["receiver_id", "po_id"],
    comment="Cleaned goods receipts confirming physical part arrival.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_receiver_id", "receiver_id IS NOT NULL")
@dp.expect_or_fail("valid_po_id", "po_id IS NOT NULL")
@dp.expect_or_drop("positive_received_qty", "received_qty > 0")
@dp.expect_or_drop("not_rejected", "quality_status != 'REJECTED'")
@dp.expect("has_warehouse", "warehouse_location IS NOT NULL")
def silver_receivers():
    return (
        spark.readStream.table("bronze_receivers")
        .filter(
            F.col("receiver_id").isNotNull()
            & F.col("po_id").isNotNull()
            & (F.col("received_qty") > 0)
            & (F.col("quality_status") != "REJECTED")
        )
        .select(
            F.col("receiver_id"),
            F.col("po_id"),
            F.col("part_id"),
            F.col("received_date").cast(DateType()).alias("received_date"),
            F.col("received_qty").cast(IntegerType()).alias("received_qty"),
            F.col("warehouse_location"),
            F.upper(F.col("quality_status")).alias("quality_status"),
            F.col("inspector_id"),
            F.col("created_at"),
        )
    )


# ---------------------------------------------------------------------------
# Supplier Invoices (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_invoices",
    cluster_by=["invoice_id", "po_id"],
    comment="Cleaned supplier invoices completing the procure-to-pay chain.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_invoice_id", "invoice_id IS NOT NULL")
@dp.expect_or_fail("valid_po_id", "po_id IS NOT NULL")
@dp.expect_or_drop("positive_amount", "invoice_amount > 0")
@dp.expect("valid_payment_status", "payment_status IN ('PAID', 'PENDING', 'OVERDUE', 'CANCELLED')")
@dp.expect("reasonable_payment_days", "days_to_payment BETWEEN 0 AND 365")
def silver_invoices():
    return (
        spark.readStream.table("bronze_invoices")
        .filter(
            F.col("invoice_id").isNotNull()
            & F.col("po_id").isNotNull()
            & (F.col("invoice_amount") > 0)
        )
        .select(
            F.col("invoice_id"),
            F.col("po_id"),
            F.col("receiver_id"),
            F.col("supplier_id"),
            F.col("invoice_date").cast(DateType()).alias("invoice_date"),
            F.col("invoice_amount").cast(DecimalType(14, 2)).alias("invoice_amount"),
            F.upper(F.col("payment_status")).alias("payment_status"),
            F.col("payment_date").cast(DateType()).alias("payment_date"),
            F.col("days_to_payment").cast(IntegerType()).alias("days_to_payment"),
            F.col("created_at"),
        )
    )


# ---------------------------------------------------------------------------
# Field Work Orders (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_work_orders",
    cluster_by=["wo_id", "customer_id"],
    comment="Cleaned field work orders. Parts attach here; drives customer quotes and completion dates.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_wo_id", "wo_id IS NOT NULL")
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_priority", "priority IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')")
@dp.expect("valid_wo_status", "wo_status IN ('OPEN', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED', 'PENDING')")
@dp.expect("scheduled_after_created", "scheduled_date >= created_date")
def silver_work_orders():
    return (
        spark.readStream.table("bronze_work_orders")
        .filter(
            F.col("wo_id").isNotNull() & F.col("customer_id").isNotNull()
        )
        .select(
            F.col("wo_id"),
            F.col("customer_id"),
            F.col("part_id"),
            F.col("technician_id"),
            F.col("created_date").cast(DateType()).alias("created_date"),
            F.col("scheduled_date").cast(DateType()).alias("scheduled_date"),
            F.col("wo_type"),
            F.upper(F.col("priority")).alias("priority"),
            F.upper(F.col("wo_status")).alias("wo_status"),
            F.col("description"),
            F.col("region"),
            F.col("created_at"),
        )
    )


# ---------------------------------------------------------------------------
# Customer Quotes (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_customer_quotes",
    cluster_by=["quote_id", "customer_id"],
    comment="Cleaned customer quotes. Expected completion date driven by parts + labor availability.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_quote_id", "quote_id IS NOT NULL")
@dp.expect_or_fail("valid_wo_id", "wo_id IS NOT NULL")
@dp.expect_or_drop("positive_total", "total_amount > 0")
@dp.expect("parts_plus_labor_equals_total", "ABS(parts_cost + labor_cost - total_amount) < 0.01")
@dp.expect("completion_after_quote", "expected_completion_date >= quote_date")
@dp.expect("valid_quote_status", "quote_status IN ('DRAFT', 'SENT', 'ACCEPTED', 'REJECTED', 'EXPIRED')")
def silver_customer_quotes():
    return (
        spark.readStream.table("bronze_customer_quotes")
        .filter(
            F.col("quote_id").isNotNull()
            & F.col("wo_id").isNotNull()
            & (F.col("total_amount") > 0)
        )
        .select(
            F.col("quote_id"),
            F.col("wo_id"),
            F.col("customer_id"),
            F.col("part_id"),
            F.col("quote_date").cast(DateType()).alias("quote_date"),
            F.col("parts_cost").cast(DecimalType(12, 2)).alias("parts_cost"),
            F.col("labor_cost").cast(DecimalType(12, 2)).alias("labor_cost"),
            F.col("total_amount").cast(DecimalType(14, 2)).alias("total_amount"),
            F.col("expected_completion_date").cast(DateType()).alias("expected_completion_date"),
            F.col("availability_constraint"),
            F.col("labor_constraint"),
            F.upper(F.col("quote_status")).alias("quote_status"),
            F.datediff(
                F.col("expected_completion_date"), F.col("quote_date")
            ).alias("days_to_completion"),
            F.col("created_at"),
        )
    )


# ---------------------------------------------------------------------------
# Labor Schedules (cleaned)
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_labor_schedules",
    cluster_by=["technician_id", "schedule_date"],
    comment="Cleaned technician schedules. Available hours govern quote completion dates.",
    table_properties={"quality": "silver", "layer": "transformation"},
)
@dp.expect_or_fail("valid_schedule_id", "schedule_id IS NOT NULL")
@dp.expect_or_fail("valid_technician_id", "technician_id IS NOT NULL")
@dp.expect_or_drop("non_negative_hours", "available_hours >= 0")
@dp.expect("booked_within_available", "booked_hours <= available_hours")
@dp.expect("non_negative_remaining", "remaining_hours >= 0")
@dp.expect("has_schedule_date", "schedule_date IS NOT NULL")
def silver_labor_schedules():
    return (
        spark.readStream.table("bronze_labor_schedules")
        .filter(
            F.col("schedule_id").isNotNull()
            & F.col("technician_id").isNotNull()
            & (F.col("available_hours") >= 0)
        )
        .select(
            F.col("schedule_id"),
            F.col("technician_id"),
            F.trim(F.col("technician_name")).alias("technician_name"),
            F.col("schedule_date").cast(DateType()).alias("schedule_date"),
            F.col("available_hours").cast(DecimalType(5, 2)).alias("available_hours"),
            F.col("booked_hours").cast(DecimalType(5, 2)).alias("booked_hours"),
            (F.col("available_hours") - F.col("booked_hours"))
            .cast(DecimalType(5, 2))
            .alias("remaining_hours"),
            F.upper(F.col("region")).alias("region"),
            F.col("skill_level"),
            F.col("created_at"),
        )
    )
