# FinGuard — Databricks Setup Guide
## From Zero to Running Pipeline in ~30 Minutes

---

## Step 1 — Unity Catalog Structure

Run this in any Databricks notebook to create the full catalog structure:

```sql
-- Cell 1: Create catalog
CREATE CATALOG IF NOT EXISTS finguard
COMMENT 'FinGuard fraud detection pipeline — AU banking data';

-- Cell 2: Create schemas (one per Medallion layer)
CREATE SCHEMA IF NOT EXISTS finguard.raw
COMMENT 'Raw source files — Unity Catalog Volume';

CREATE SCHEMA IF NOT EXISTS finguard.bronze
COMMENT 'Bronze layer — raw Delta tables with audit columns';

CREATE SCHEMA IF NOT EXISTS finguard.silver
COMMENT 'Silver layer — cleaned and enriched Delta tables';

CREATE SCHEMA IF NOT EXISTS finguard.gold
COMMENT 'Gold layer — business-ready fraud feature tables';

CREATE SCHEMA IF NOT EXISTS finguard.monitoring
COMMENT 'Pipeline monitoring — DQ results, dead-letter table';

-- Cell 3: Create the Volume for CSV uploads
CREATE VOLUME IF NOT EXISTS finguard.raw.source_files
COMMENT 'Source CSV files from payment gateway feeds';
```

---

## Step 2 — Upload Data Files

### Via Databricks UI (recommended for first time)

1. Left sidebar → **Catalog**
2. Navigate: `finguard` → `raw` → `source_files`
3. Click **"Upload to this volume"**
4. Upload these 3 files:
   - `data/raw/transactions/transactions.csv`
   - `data/raw/customers/customers.csv`
   - `data/raw/merchants/merchants.csv`

Your files will be at:
```
/Volumes/finguard/raw/source_files/transactions.csv
/Volumes/finguard/raw/source_files/customers.csv
/Volumes/finguard/raw/source_files/merchants.csv
```

### Via Databricks CLI (use this in interviews — more impressive)

```bash
# Install CLI
pip install databricks-cli

# Configure (get token from Databricks UI → Settings → Developer → Access Tokens)
databricks configure --token

# Upload files
databricks fs cp data/raw/transactions/transactions.csv \
    /Volumes/finguard/raw/source_files/transactions.csv

databricks fs cp data/raw/customers/customers.csv \
    /Volumes/finguard/raw/source_files/customers.csv

databricks fs cp data/raw/merchants/merchants.csv \
    /Volumes/finguard/raw/source_files/merchants.csv
```

---

## Step 3 — Connect GitHub to Databricks

This is how real teams version-control notebooks. Interviewers love this.

### 3a. Create GitHub Personal Access Token
1. GitHub → Settings → Developer Settings → Personal Access Tokens → Tokens (classic)
2. Scopes: tick `repo` and `workflow`
3. Copy the token (shown only once)

### 3b. Link Databricks to GitHub
1. Databricks → top-right user icon → **Settings**
2. **User → Linked accounts**
3. Git provider: **GitHub**
4. Paste your Personal Access Token → **Save**

### 3c. Create a Databricks Repo
1. Left sidebar → **Workspace** → **Repos**
2. Click **Add Repo**
3. Paste: `https://github.com/yourusername/finguard-fraud-pipeline`
4. Click **Create Repo**

Any notebook saved in this repo folder automatically syncs to GitHub.

---

## Step 4 — Verify Setup

Run this in a notebook to confirm everything is wired up:

```python
# Cell 1 — Spark version
print(f"Spark version: {spark.version}")

# Cell 2 — Verify Volume files exist
import os
volume_path = "/Volumes/finguard/raw/source_files"
files = dbutils.fs.ls(volume_path)
for f in files:
    size_mb = f.size / (1024 * 1024)
    print(f"  {f.name:<30} {size_mb:.1f} MB")

# Cell 3 — Quick read test
df = spark.read.option("header", "true").csv(
    f"{volume_path}/transactions.csv"
)
print(f"\nTransactions loaded: {df.count():,}")
```

Expected output:
```
Spark version: 3.5.0

  transactions.csv               118.4 MB
  customers.csv                    1.2 MB
  merchants.csv                    0.1 MB

Transactions loaded: 500,000
```

---

## Step 5 — Run the Notebooks

Open and run in order, connecting to **Serverless** compute:

```
notebooks/01_bronze_ingestion.ipynb         ← Start here
notebooks/02_silver_transformation.ipynb
notebooks/03_gold_feature_engineering.ipynb
```

To connect to Serverless:
- Open a notebook → top right → **Connect** → **Serverless**
- Cells run immediately — no cluster startup wait

---

## Unity Catalog Naming Reference

| Object | Name | Full Path |
|--------|------|-----------|
| Catalog | `finguard` | `finguard` |
| Raw schema | `raw` | `finguard.raw` |
| Bronze schema | `bronze` | `finguard.bronze` |
| Silver schema | `silver` | `finguard.silver` |
| Gold schema | `gold` | `finguard.gold` |
| Volume | `source_files` | `/Volumes/finguard/raw/source_files/` |
| Bronze transactions | `transactions` | `finguard.bronze.transactions` |
| Bronze customers | `customers` | `finguard.bronze.customers` |
| Bronze merchants | `merchants` | `finguard.bronze.merchants` |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `SCHEMA_NOT_FOUND` | Run the CREATE SCHEMA cells in Step 1 first |
| `VOLUME_NOT_FOUND` | Run `CREATE VOLUME IF NOT EXISTS finguard.raw.source_files` |
| File not found in Volume | Re-upload via Catalog → finguard → raw → source_files |
| Notebook says "Detached" | Top-right → Connect → Serverless |
| GitHub push fails | Check token has `repo` scope |
| `AnalysisException: Table not found` | Run earlier notebooks first — tables created in order |

---

## What to Say in an Interview

> *"I built a Medallion Architecture pipeline on Databricks using Unity Catalog for  
> data governance, with separate Bronze, Silver, and Gold schemas. Raw CSVs land in  
> a managed Unity Catalog Volume, get ingested into Bronze Delta tables with explicit  
> schema enforcement and audit columns, then transformed through Silver for cleaning  
> and enrichment, and finally aggregated into Gold fraud feature tables using Delta  
> MERGE for incremental upserts."*
