# Databricks notebook source
# MAGIC %md
# MAGIC # UC Governance Demo — Pella Workshop
# MAGIC ## Row-Level Security + Column Masking
# MAGIC
# MAGIC **Run cell by cell. Do not run all at once.**
# MAGIC
# MAGIC The story: one table, two mapping tables, and identity-based access control applied live.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "mfg_mc_se_sa", "Catalog")
dbutils.widgets.text("schema", "pella", "Schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog};
# MAGIC USE SCHEMA ${schema};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: The table today — no governance applied
# MAGIC
# MAGIC ~27,000 work orders across 5 regions. All cost columns visible to everyone. No access control.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   wo_id, customer_name, region, wo_status, priority,
# MAGIC   parts_cost, labor_cost, total_amount,
# MAGIC   expected_completion_date
# MAGIC FROM demo_work_orders
# MAGIC ORDER BY created_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, COUNT(*) AS work_orders
# MAGIC FROM demo_work_orders
# MAGIC GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: The policy — who sees what
# MAGIC
# MAGIC Two mapping tables define access. Updating policy is an INSERT or UPDATE — no code changes, no redeployment.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Regional access: current user is mapped to MIDWEST only
# MAGIC SELECT * FROM region_access;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cost access: empty — no one is authorized to see cost data yet
# MAGIC SELECT * FROM cost_access;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply row-level security
# MAGIC
# MAGIC One statement. From this point on, every query against this table is automatically
# MAGIC filtered based on the caller's identity — no application changes required.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_work_orders
# MAGIC SET ROW FILTER pella_region_filter ON (region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same query. Same table. Different result.
# MAGIC SELECT
# MAGIC   wo_id, customer_name, region, wo_status, priority,
# MAGIC   parts_cost, labor_cost, total_amount,
# MAGIC   expected_completion_date
# MAGIC FROM demo_work_orders
# MAGIC ORDER BY created_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count confirms: only MIDWEST rows are visible
# MAGIC SELECT region, COUNT(*) AS work_orders
# MAGIC FROM demo_work_orders
# MAGIC GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply column masking
# MAGIC
# MAGIC Cost data is sensitive — finance team only. Three cost columns, two mask functions (matching column types).

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE demo_work_orders
# MAGIC   ALTER COLUMN parts_cost SET MASK pella_mask_cost;
# MAGIC
# MAGIC ALTER TABLE demo_work_orders
# MAGIC   ALTER COLUMN labor_cost SET MASK pella_mask_cost;
# MAGIC
# MAGIC ALTER TABLE demo_work_orders
# MAGIC   ALTER COLUMN total_amount SET MASK pella_mask_total;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MIDWEST rows only. Cost columns are now NULL — not authorized.
# MAGIC SELECT
# MAGIC   wo_id, customer_name, region, wo_status, priority,
# MAGIC   parts_cost, labor_cost, total_amount,
# MAGIC   expected_completion_date
# MAGIC FROM demo_work_orders
# MAGIC ORDER BY created_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Grant cost access
# MAGIC
# MAGIC One INSERT. No code change. No redeployment. Takes effect immediately.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cost_access SELECT current_user();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MIDWEST rows. Costs now visible.
# MAGIC SELECT
# MAGIC   wo_id, customer_name, region, wo_status, priority,
# MAGIC   parts_cost, labor_cost, total_amount,
# MAGIC   expected_completion_date
# MAGIC FROM demo_work_orders
# MAGIC ORDER BY created_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Grant full regional access
# MAGIC
# MAGIC Change one value in the mapping table. All regions become visible.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE region_access
# MAGIC SET allowed_region = 'ALL'
# MAGIC WHERE user_name = current_user();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full table. All regions, all costs.
# MAGIC SELECT
# MAGIC   wo_id, customer_name, region, wo_status, priority,
# MAGIC   parts_cost, labor_cost, total_amount,
# MAGIC   expected_completion_date
# MAGIC FROM demo_work_orders
# MAGIC ORDER BY created_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, COUNT(*) AS work_orders
# MAGIC FROM demo_work_orders
# MAGIC GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Now let's talk about your data
# MAGIC
# MAGIC - **What dimensions would you filter on?** Region, plant, business unit, cost center?
# MAGIC - **Which columns are sensitive?** Cost data, customer PII, pricing, supplier terms?
# MAGIC - **Who are the personas?** What does each role need to see?
# MAGIC
# MAGIC The mapping tables and functions are the only things that change. The table definition stays the same.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC ## REFERENCE: Group-based RLS and Column Masking (Production Pattern)
# MAGIC
# MAGIC The mapping table approach above is a practical workaround when you don't have account admin
# MAGIC privileges to manage UC groups. In production, the recommended pattern is to drive access
# MAGIC control from **Unity Catalog account groups** instead of a custom mapping table.
# MAGIC
# MAGIC This is cleaner, more scalable, and integrates with your identity provider (AAD, Okta, etc.)
# MAGIC via SCIM sync. Access changes by adding/removing users from groups — no table updates required.
# MAGIC
# MAGIC The ALTER TABLE syntax is identical. Only the functions change.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Pattern: Group-based Row Filter
# MAGIC
# MAGIC Instead of a mapping table, the filter function checks UC account group membership directly.
# MAGIC Requires groups to exist in the account (e.g. `pella_admin`, `pella_region_midwest`).
# MAGIC The `ALTER TABLE` syntax to apply it is identical — only the function body changes.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION pella_region_filter_groups(region STRING)
# MAGIC RETURN
# MAGIC   IS_ACCOUNT_GROUP_MEMBER('pella_admin')
# MAGIC   OR
# MAGIC   IS_ACCOUNT_GROUP_MEMBER('pella_region_' || LOWER(region));
# MAGIC
# MAGIC -- Apply it:
# MAGIC ALTER TABLE demo_work_orders
# MAGIC SET ROW FILTER pella_region_filter_groups ON (region);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Pattern: Group-based Column Mask
# MAGIC
# MAGIC Same idea for column masking — check group membership instead of a lookup table.
# MAGIC Members of `pella_finance` or `pella_admin` see real values; everyone else gets NULL.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION pella_mask_cost_groups(cost DECIMAL(12,2))
# MAGIC RETURN IF(
# MAGIC   IS_ACCOUNT_GROUP_MEMBER('pella_finance') OR IS_ACCOUNT_GROUP_MEMBER('pella_admin'),
# MAGIC   cost,
# MAGIC   NULL
# MAGIC );
# MAGIC
# MAGIC -- Apply it:
# MAGIC ALTER TABLE demo_work_orders
# MAGIC   ALTER COLUMN parts_cost SET MASK pella_mask_cost_groups,
# MAGIC   ALTER COLUMN labor_cost SET MASK pella_mask_cost_groups;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key differences vs. the mapping table approach
# MAGIC
# MAGIC | | Mapping Table | UC Groups |
# MAGIC |---|---|---|
# MAGIC | **Who manages access** | Anyone with MODIFY on the mapping table | Account admin or group manager |
# MAGIC | **How access changes** | INSERT / UPDATE / DELETE | Add/remove user from group |
# MAGIC | **IdP integration** | Manual | SCIM sync (AAD, Okta) |
# MAGIC | **Audit trail** | Delta table history | UC audit logs |
# MAGIC | **Scale** | Works, but table grows with users | Designed for large user bases |
# MAGIC | **Requires account admin** | No | Yes (to create groups) |
# MAGIC
# MAGIC For Pella's environment, the group-based approach is the production target.
# MAGIC The mapping table is a stepping stone — useful today, easy to replace once groups are set up.
