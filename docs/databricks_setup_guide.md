# FinGuard — Databricks Setup Guide

This guide walks through setting up the FinGuard fraud detection pipeline from a fresh Databricks workspace, using Unity Catalog and Serverless compute.

---

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Access to Serverless compute (used throughout this project)
- The three source CSV files: `transactions.csv`, `customers.csv`, `merchants.csv`

---

## Step 1 — Run the Environment Setup Notebook

Open `notebooks/setup/00_environment_setup.ipynb` and run it once, manually. It creates the Unity Catalog structure and the pipeline control (watermark) table:

```sql
CREATE CATALOG IF NOT EXISTS finguard;

CREATE SCHEMA IF NOT EXISTS finguard.raw;
CREATE SCHEMA IF NOT EXISTS finguard.bronze;
CREATE SCHEMA IF NOT EXISTS finguard.silver;
CREATE SCHEMA IF NOT EXISTS finguard.gold;
CREATE SCHEMA IF NOT EXISTS finguard.monitoring;

CREATE VOLUME IF NOT EXISTS finguard.raw.source_files;
```

This notebook is intentionally separate from the daily pipeline notebooks — it's idempotent and safe to re-run, but it's not part of the scheduled workflow.

---

## Step 2 — Upload Source Data

### Via Databricks UI

1. Left sidebar → **Catalog**
2. Navigate: `finguard` → `raw` → `source_files`
3. Click **"Upload to this volume"**
4. Upload the three CSV files

Files will be available at:
```
/Volumes/finguard/raw/source_files/transactions.csv
/Volumes/finguard/raw/source_files/customers.csv
/Volumes/finguard/raw/source_files/merchants.csv
```

### Via Databricks CLI

```bash
databricks auth login --host <your-workspace-url>

databricks fs cp data/raw/transactions/transactions.csv \
    dbfs:/Volumes/finguard/raw/source_files/transactions.csv

databricks fs cp data/raw/customers/customers.csv \
    dbfs:/Volumes/finguard/raw/source_files/customers.csv

databricks fs cp data/raw/merchants/merchants.csv \
    dbfs:/Volumes/finguard/raw/source_files/merchants.csv
```

---

## Step 3 — Connect GitHub to Databricks

1. **Create a GitHub Personal Access Token**
   GitHub → Settings → Developer Settings → Personal Access Tokens → Tokens (classic), with `repo` scope.

2. **Link the token in Databricks**
   Databricks → user icon → Settings → Linked accounts → Git provider: GitHub → paste token.

3. **Clone the repo into Databricks Repos**
   Left sidebar → Workspace → Repos → Add Repo → paste the repository URL.

Notebooks saved inside this Repos folder sync directly with GitHub.

---

## Step 4 — Run the Pipeline Notebooks

Run in order, connected to **Serverless** compute:

```
notebooks/pipeline/01_bronze_ingestion.ipynb
notebooks/pipeline/02_silver_transformation.ipynb
notebooks/pipeline/03_gold_feature_engineering.ipynb
```

To connect to Serverless: open a notebook → top right → **Connect** → **Serverless**.

---

## Step 5 — Deploy as a Scheduled Workflow

The pipeline runs as a Databricks Job with three dependent tasks (Bronze → Silver → Gold), each running on Serverless compute, scheduled daily.

The job is also defined as code using **Databricks Asset Bundles**, in `databricks.yml` and `resources/finguard_pipeline_workflow.job.yml`. To deploy it from the CLI:

```bash
databricks bundle validate
databricks bundle deploy
```

---

## Unity Catalog Reference

| Object | Name | Full Path |
|---|---|---|
| Catalog | `finguard` | `finguard` |
| Raw schema | `raw` | `finguard.raw` |
| Bronze schema | `bronze` | `finguard.bronze` |
| Silver schema | `silver` | `finguard.silver` |
| Gold schema | `gold` | `finguard.gold` |
| Monitoring schema | `monitoring` | `finguard.monitoring` |
| Volume | `source_files` | `/Volumes/finguard/raw/source_files/` |
| Bronze transactions | `transactions` | `finguard.bronze.transactions` |
| Bronze customers | `customers` | `finguard.bronze.customers` |
| Bronze merchants | `merchants` | `finguard.bronze.merchants` |

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `SCHEMA_NOT_FOUND` | Run `00_environment_setup.ipynb` first |
| `VOLUME_NOT_FOUND` | Re-run the `CREATE VOLUME` statement in Step 1 |
| File not found in Volume | Re-upload via Catalog → finguard → raw → source_files |
| Notebook shows "Detached" | Top right → Connect → Serverless |
| GitHub push fails | Confirm the linked token has `repo` scope |
| `AnalysisException: Table not found` | Run notebooks in order — each layer depends on the one before it |
| `bundle deploy` variable errors on Windows | Set `$env:BUNDLE_VAR_notification_email = "your@email.com"` in PowerShell, then run `databricks bundle deploy` with no `--var` flag |