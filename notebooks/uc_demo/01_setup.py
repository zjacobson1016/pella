# Databricks notebook source
# MAGIC %md
# MAGIC # UC Demo Setup - Pella Workshop
# MAGIC
# MAGIC **Run this notebook before the demo session, not during it.**
# MAGIC
# MAGIC Creates:
# MAGIC - `demo_work_orders` — plain Delta table used for the demo (CTAS from fact_work_order_completion)
# MAGIC - `region_access` and `cost_access` mapping tables
# MAGIC - Row filter function (`pella_region_filter`)
# MAGIC - Column mask functions (`pella_mask_cost`, `pella_mask_total`)
# MAGIC
# MAGIC The functions are NOT applied to the table here — that happens live in the demo notebook.
# MAGIC
# MAGIC **Note:** `fact_work_order_completion` is a materialized view and `silver_*` tables are streaming tables.
# MAGIC UC row filters and column masks are supported on both types, but using a plain Delta table
# MAGIC here eliminates any uncertainty for the demo. We can discuss MV/streaming table support separately.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Defaults match the bundle. Override if the pipeline deployed to a different catalog/schema.

# COMMAND ----------

dbutils.widgets.text("catalog", "mfg_mc_se_sa", "Catalog")
dbutils.widgets.text("schema", "pella", "Schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog};
# MAGIC USE SCHEMA ${schema};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create demo table (plain Delta)
# MAGIC
# MAGIC A regular managed Delta table with the same data as the fact MV.
# MAGIC This is the table we'll apply governance to during the demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo_work_orders AS
# MAGIC SELECT
# MAGIC   wo_id, customer_name, region, wo_status, priority,
# MAGIC   parts_cost, labor_cost, total_amount,
# MAGIC   expected_completion_date, created_date,
# MAGIC   wo_type, customer_tier, customer_region,
# MAGIC   days_to_completion, quote_status
# MAGIC FROM fact_work_order_completion;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify
# MAGIC SELECT region, COUNT(*) AS work_orders
# MAGIC FROM demo_work_orders
# MAGIC GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create mapping tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE region_access (
# MAGIC   user_name STRING,
# MAGIC   allowed_region STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cost_access (
# MAGIC   user_name STRING
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Set starting state
# MAGIC
# MAGIC Current user mapped to MIDWEST only. Cost access table is empty — no one can see cost data yet.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM region_access;
# MAGIC DELETE FROM cost_access;
# MAGIC
# MAGIC INSERT INTO region_access SELECT current_user(), 'MIDWEST';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify starting state
# MAGIC SELECT 'region_access' AS tbl, * FROM region_access
# MAGIC UNION ALL
# MAGIC SELECT 'cost_access', user_name, NULL FROM cost_access;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create row filter function
# MAGIC
# MAGIC Returns TRUE (show row) if the current user has a matching entry in `region_access`.
# MAGIC - No entry = see nothing
# MAGIC - Entry with specific region = see only that region
# MAGIC - Entry with 'ALL' = see everything

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION pella_region_filter(region STRING)
# MAGIC RETURN EXISTS (
# MAGIC   SELECT 1 FROM region_access
# MAGIC   WHERE user_name = CURRENT_USER()
# MAGIC   AND (allowed_region = region OR allowed_region = 'ALL')
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create column mask functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Applied to parts_cost and labor_cost (DECIMAL 12,2)
# MAGIC CREATE OR REPLACE FUNCTION pella_mask_cost(cost DECIMAL(12,2))
# MAGIC RETURN IF(
# MAGIC   EXISTS (SELECT 1 FROM cost_access WHERE user_name = CURRENT_USER()),
# MAGIC   cost,
# MAGIC   NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Applied to total_amount (DECIMAL 14,2)
# MAGIC CREATE OR REPLACE FUNCTION pella_mask_total(cost DECIMAL(14,2))
# MAGIC RETURN IF(
# MAGIC   EXISTS (SELECT 1 FROM cost_access WHERE user_name = CURRENT_USER()),
# MAGIC   cost,
# MAGIC   NULL
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC Open the demo notebook and run it cell by cell during the session.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## RESET DEMO
# MAGIC
# MAGIC Run these cells to get back to the starting state and run the demo again.
# MAGIC Drops the row filter and column masks from the table, and resets the mapping tables.
# MAGIC The demo_work_orders data itself is untouched.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_work_orders DROP ROW FILTER;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_work_orders ALTER COLUMN parts_cost DROP MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_work_orders ALTER COLUMN labor_cost DROP MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_work_orders ALTER COLUMN total_amount DROP MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM region_access;
# MAGIC DELETE FROM cost_access;
# MAGIC
# MAGIC INSERT INTO region_access SELECT current_user(), 'MIDWEST';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify reset state
# MAGIC SELECT * FROM region_access;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cost_access;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## FULL CLEANUP
# MAGIC
# MAGIC Run these cells when completely done with the workshop.
# MAGIC No need to drop filters/masks first — they go away with the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_work_orders;
# MAGIC DROP TABLE IF EXISTS region_access;
# MAGIC DROP TABLE IF EXISTS cost_access;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS pella_region_filter;
# MAGIC DROP FUNCTION IF EXISTS pella_mask_cost;
# MAGIC DROP FUNCTION IF EXISTS pella_mask_total;
