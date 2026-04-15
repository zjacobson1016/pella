# =============================================================================
# BRONZE LAYER — AutoLoader ingestion from Unity Catalog volume
# All tables are append-only streaming tables that read raw Parquet files.
# Metadata columns (_source_file, _ingested_at) are added at this layer.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


# ---------------------------------------------------------------------------
# Parts CDC — INSERT / UPDATE / DELETE events for SCD dimensions
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_parts",
    cluster_by=["part_id"],
    comment="Raw parts CDC events from source ERP system. Feeds SCD Type 1 and Type 2 gold dimensions.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_parts():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "part_id STRING, part_number STRING, part_name STRING, "
            "category STRING, subcategory STRING, piece_part_price DOUBLE, "
            "standard_cost DOUBLE, lead_time_days INT, supplier_id STRING, "
            "unit_of_measure STRING, is_active BOOLEAN, "
            "_op STRING, _sequence BIGINT, updated_at TIMESTAMP",
        )
        .load(f"{raw_path}/parts_cdc/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Customers CDC — INSERT / UPDATE / DELETE events for SCD dimensions
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_customers",
    cluster_by=["customer_id"],
    comment="Raw customer CDC events from CRM. Feeds SCD Type 1 and Type 2 gold dimensions.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_customers():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "customer_id STRING, customer_name STRING, address STRING, "
            "city STRING, state STRING, zip STRING, customer_tier STRING, "
            "account_manager STRING, region STRING, annual_revenue DOUBLE, "
            "is_active BOOLEAN, _op STRING, _sequence BIGINT, updated_at TIMESTAMP",
        )
        .load(f"{raw_path}/customers_cdc/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Demand Signals
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_demand_signals",
    cluster_by=["part_id", "forecast_date"],
    comment="Raw demand forecast signals. Foundation of the demand-driven supply chain model.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_demand_signals():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "signal_id STRING, part_id STRING, forecast_date DATE, "
            "forecasted_qty INT, actual_qty INT, signal_type STRING, "
            "confidence_score DOUBLE, region STRING, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/demand_signals/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Purchase Orders
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_purchase_orders",
    cluster_by=["po_id", "part_id"],
    comment="Raw purchase orders. Downstream of demand signals in the supply chain.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_purchase_orders():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "po_id STRING, signal_id STRING, part_id STRING, supplier_id STRING, "
            "po_date DATE, expected_receipt_date DATE, ordered_qty INT, "
            "unit_price DOUBLE, po_status STRING, buyer_id STRING, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/purchase_orders/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Receivers (Goods Receipts)
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_receivers",
    cluster_by=["receiver_id", "po_id"],
    comment="Raw goods receipts. Confirms physical part arrival against POs.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_receivers():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "receiver_id STRING, po_id STRING, part_id STRING, "
            "received_date DATE, received_qty INT, warehouse_location STRING, "
            "quality_status STRING, inspector_id STRING, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/receivers/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Supplier Invoices
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_invoices",
    cluster_by=["invoice_id", "po_id"],
    comment="Raw supplier invoices. Completes the PO-to-payment financial chain.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_invoices():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "invoice_id STRING, po_id STRING, receiver_id STRING, supplier_id STRING, "
            "invoice_date DATE, invoice_amount DOUBLE, payment_status STRING, "
            "payment_date DATE, days_to_payment INT, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/invoices/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Field Work Orders
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_work_orders",
    cluster_by=["wo_id", "customer_id"],
    comment="Raw field work orders. Parts attach here, driving inventory consumption and customer quotes.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_work_orders():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "wo_id STRING, customer_id STRING, part_id STRING, technician_id STRING, "
            "created_date DATE, scheduled_date DATE, wo_type STRING, priority STRING, "
            "wo_status STRING, description STRING, region STRING, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/work_orders/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Customer Quotes
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_customer_quotes",
    cluster_by=["quote_id", "customer_id"],
    comment="Raw customer quotes. Completion date governed by part availability, pricing, and labor.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_customer_quotes():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "quote_id STRING, wo_id STRING, customer_id STRING, part_id STRING, "
            "quote_date DATE, parts_cost DOUBLE, labor_cost DOUBLE, total_amount DOUBLE, "
            "expected_completion_date DATE, availability_constraint STRING, "
            "labor_constraint STRING, quote_status STRING, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/customer_quotes/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )


# ---------------------------------------------------------------------------
# Labor Schedules
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_labor_schedules",
    cluster_by=["technician_id", "schedule_date"],
    comment="Raw technician labor schedules. Availability drives expected completion date on customer quotes.",
    table_properties={"quality": "bronze", "layer": "ingestion"},
)
def bronze_labor_schedules():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaHints",
            "schedule_id STRING, technician_id STRING, technician_name STRING, "
            "schedule_date DATE, available_hours DOUBLE, booked_hours DOUBLE, "
            "region STRING, skill_level STRING, created_at TIMESTAMP",
        )
        .load(f"{raw_path}/labor_schedules/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )
