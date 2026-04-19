# UC Governance Demo

Row-level security and column masking layered on top of the bundle's gold layer. Shown live during the Pella workshop.

## Prerequisites

1. The bundle's pipeline (`pella_parts_forecasting`) has run at least once so `fact_work_order_completion` exists.
2. You can create tables, views, and functions in the target schema.

## Run order

1. `01_setup.py`, once before the session. Creates `demo_work_orders`, the `region_access` / `cost_access` mapping tables, and the row filter / column mask functions. Also contains reset and full-cleanup sections at the bottom.
2. `02_demo.py`, step through cell by cell during the session. Applies RLS and column masking live, then grants cost and region access to show policy changes without redeploying code.

## Parameters

Both notebooks expose `catalog` and `schema` widgets. Defaults are `mfg_mc_se_sa` and `pella`, matching the bundle. Override if you deploy elsewhere.

## Identity

The mapping tables are seeded with `current_user()`, so whoever runs the setup is wired in as the demo user. Works in rehearsal and for anyone who checks out the repo and runs it themselves.

## Reference section

The demo notebook ends with a reference showing the same pattern implemented with UC account groups and `IS_ACCOUNT_GROUP_MEMBER`. That's the production target; the mapping table version is a stepping stone for environments without account admin access.
