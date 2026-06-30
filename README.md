<div align="center">

# 🛡️ FinGuard
### End-to-End Fraud Detection Data Pipeline

**Production-grade PySpark & Databricks pipeline modelled on Australian retail banking**  
*Patterns based on CBA / NAB / Westpac data engineering practices*

---

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Databricks](https://img.shields.io/badge/Databricks-14.3_LTS-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-003366?style=for-the-badge)](https://delta.io)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-enabled-00A971?style=for-the-badge)](https://docs.databricks.com/data-governance/unity-catalog)

</div>

---

## 📋 Overview

FinGuard is a financial data engineering pipeline that ingests, transforms, and feature-engineers **500,000 synthetic Australian banking transactions** to support fraud detection at scale.

The pipeline is built using the **Medallion Architecture** (Bronze → Silver → Gold) on Databricks with Delta Lake as the storage layer and Unity Catalog for governance — the same stack used by Australian banks on their cloud data platforms. It runs end to end as a scheduled Databricks Workflow, deployed declaratively with Databricks Asset Bundles.

### Why This Project?

Australian banks process over **$1 trillion in card transactions annually** (RBA Payments Data). Fraud detection pipelines at CBA, NAB, Westpac, and ANZ rely on exactly the patterns demonstrated here: scalable incremental ingestion, enriched feature tables, and risk scoring using Delta Lake and Spark.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                   │
│                                                                          │
│   Payment Gateway Feed      Core Banking System      Customer Master     │
│   (Visa DPS / eftpos AU)    (transactions.csv)       (customers.csv)     │
│          │                         │                        │            │
└──────────┼─────────────────────────┼────────────────────────┼────────────┘
           │                         │                        │
           ▼                         ▼                        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              /Volumes/finguard/raw/source_files/                         │
│                   Unity Catalog Managed Volume                           │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                    🥉  BRONZE LAYER  (finguard.bronze.*)                 ║
║                                                                          ║
║  • Explicit StructType schema enforcement (no inferSchema)               ║
║  • PERMISSIVE ingestion with row-level data quality checks               ║
║  • Audit columns: _ingested_at, _batch_id, _source_file                  ║
║  • Partitioned by partition_date for query pruning                       ║
║  • OPTIMIZE + ZORDER BY (customer_id, txn_date)                          ║
╚══════════════════════════════════════════════════════════════════════════╝
                               │
                               ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                    🥈  SILVER LAYER  (finguard.silver.*)                 ║
║                                                                          ║
║  • Type casting & data standardisation                                   ║
║  • Deduplication (transaction_id idempotency, window-based)              ║
║  • Broadcast joins: transactions ↔ customers ↔ merchants                 ║
║  • Window functions: rolling 7/30-day spend, velocity, sequencing        ║
║  • Bad row quarantine to finguard.monitoring.dead_letter                 ║
╚══════════════════════════════════════════════════════════════════════════╝
                               │
                               ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                    🥇  GOLD LAYER  (finguard.gold.*)                     ║
║                                                                          ║
║  • 10-signal fraud scorecard (AUSTRAC typology aligned)                  ║
║  • Composite risk_score (0–10) and risk_band                             ║
║  • Customer risk scores via Delta MERGE (incremental upsert)             ║
║  • ML-ready feature store — 35 features, no PII, binary label            ║
╚══════════════════════════════════════════════════════════════════════════╝
                               │
                               ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                    ⚙️  ORCHESTRATION  (Databricks Workflow)              ║
║                                                                          ║
║  • Bronze → Silver → Gold, scheduled daily on Serverless compute         ║
║  • Watermark-based incremental processing                                ║
║  • Deployed declaratively via Databricks Asset Bundles                   ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## 📊 Dataset

Synthetic data generated to mirror Australian retail banking patterns (AU Financial Year: Jul 2023 – Jun 2024):

| Dataset | Rows | Size | Description |
|---------|------|------|-------------|
| `transactions.csv` | 500,000 | ~120 MB | Retail banking transactions |
| `customers.csv` | 5,000 | ~1.2 MB | AU retail banking customers |
| `merchants.csv` | 800 | ~120 KB | AU merchants with MCC codes |

### 🚨 Fraud Patterns (AUSTRAC Typology Aligned)

| Pattern | Regulatory Basis | Injected Rate |
|---------|-----------------|---------------|
| Card-Not-Present (CNP) | AFCA Complaint Category | ~1.5% |
| Velocity Attack | AUSTRAC AML Typology | ~0.5% |
| Geographic Anomaly | ACCC Scamwatch | ~0.4% |
| Account Takeover (ATO) | OAIC Data Breach Reports | ~0.3% |
| Structuring / Smurfing | AUSTRAC $10,000 AUD threshold | ~0.2% |

### Merchant Categories (AS/NZS MCC Codes)

| Category | MCC | Volume |
|----------|-----|--------|
| Supermarkets & Grocery | 5411 | 22% |
| Restaurants & Cafes | 5812 | 14% |
| Online Retail | 5999 | 12% |
| Petrol & Service Station | 5541 | 10% |
| Transport & Rideshare | 4111 | 8% |
| ATM Withdrawal | 6011 | 6% |
| + 8 more categories | — | 28% |

---

## 🗂️ Unity Catalog Structure

```
finguard/                          ← Catalog
├── raw/                           ← Schema
│   └── source_files/              ← Managed Volume (CSV uploads)
│       ├── transactions.csv
│       ├── customers.csv
│       └── merchants.csv
├── bronze/                        ← Schema
│   ├── transactions               ← Delta table (partitioned by partition_date)
│   ├── customers                  ← Delta table (partitioned by address_state)
│   └── merchants                  ← Delta table
├── silver/                        ← Schema
│   ├── transactions_cleaned       ← Delta table
│   ├── transactions_enriched      ← Delta table (joined with customers + merchants)
│   └── customers                  ← Delta table
├── gold/                          ← Schema
│   ├── fraud_features             ← Delta table (per-transaction risk scoring)
│   ├── customer_risk_scores       ← Delta table (upserted via MERGE)
│   └── ml_feature_store           ← Delta table (ML-ready, no PII)
└── monitoring/                    ← Schema
    ├── pipeline_control           ← Watermark table
    └── dead_letter                ← Quarantined bad rows
```

---

## 📓 Notebooks

| Notebook | Layer | Key PySpark Concepts |
|---|---|---|
| `00_environment_setup.ipynb` | Setup | Catalog/schema creation, watermark table seeding (run once, manual) |
| `01_bronze_ingestion.ipynb` | Bronze | StructType schemas, Delta writes, partitioning, OPTIMIZE, ZORDER, data quality checks |
| `02_silver_transformation.ipynb` | Silver | Broadcast joins, window functions, deduplication, dead-letter handling |
| `03_gold_feature_engineering.ipynb` | Gold | Fraud scorecard logic, Delta MERGE, ML feature store |

---

## ⚙️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Processing Engine | Apache Spark 3.5 (PySpark) | Distributed data processing |
| Platform | Databricks Runtime 14.3 LTS, Serverless compute | Managed Spark + Delta |
| Storage Format | Delta Lake 3.0 | ACID tables with time travel |
| Data Governance | Unity Catalog | 3-level namespace, managed volumes |
| Orchestration | Databricks Workflows | Scheduled, dependency-based job execution |
| Deployment | Databricks Asset Bundles | Declarative, version-controlled job deployment |
| Language | Python 3.10 | PEP 8 compliant throughout |
| Version Control | Git + Databricks Repos | Notebooks synced directly to GitHub |

---

## 🚀 Quickstart

### Prerequisites
- A Databricks workspace with Unity Catalog enabled (Free Edition supported)
- Python 3.8+ with pip
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) (for Asset Bundle deployment)

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/finguard-fraud-pipeline.git
cd finguard-fraud-pipeline
```

### 2. Generate synthetic data
```bash
pip install -r data_generator/requirements.txt
python data_generator/generate_data.py
```

### 3. Upload CSVs to Unity Catalog

Run `notebooks/setup/00_environment_setup.ipynb` first — it creates the catalog, schemas, and volume. Then upload the three CSVs via **Catalog → finguard → raw → source_files**, or via CLI:

```bash
databricks auth login --host <your-workspace-url>
databricks fs cp data/raw/transactions/transactions.csv dbfs:/Volumes/finguard/raw/source_files/
databricks fs cp data/raw/customers/customers.csv      dbfs:/Volumes/finguard/raw/source_files/
databricks fs cp data/raw/merchants/merchants.csv      dbfs:/Volumes/finguard/raw/source_files/
```

### 4. Run the pipeline notebooks, in order, on Serverless compute

```
notebooks/setup/00_environment_setup.ipynb     ← run once
notebooks/pipeline/01_bronze_ingestion.ipynb
notebooks/pipeline/02_silver_transformation.ipynb
notebooks/pipeline/03_gold_feature_engineering.ipynb
```

### 5. Deploy as a scheduled Workflow

```bash
databricks bundle validate
databricks bundle deploy
```

Full setup walkthrough: [`docs/databricks_setup_guide.md`](docs/databricks_setup_guide.md)

---

## 🔑 Key PySpark Concepts Demonstrated

<details>
<summary><strong>DataFrame Operations</strong></summary>

- `select`, `filter`, `withColumn`, `drop`, `alias`
- `groupBy`, `agg`, `orderBy`, `distinct`
- `F.when`, `F.coalesce`, `F.lit`, `F.col`
- `F.current_timestamp`, `F.to_timestamp`, `F.date_format`
</details>

<details>
<summary><strong>Schema & Types</strong></summary>

- Explicit `StructType` / `StructField` definitions
- Why `inferSchema=False` matters in production
- Type casting and standardisation in the Silver layer
</details>

<details>
<summary><strong>Joins & Performance</strong></summary>

- Broadcast joins for small lookup tables (`F.broadcast()`)
- `repartition` vs `coalesce` — when to use each
- Partition pruning via `partitionBy`
</details>

<details>
<summary><strong>Window Functions</strong></summary>

- `Window.partitionBy().orderBy()`, `rangeBetween` vs `rowsBetween`
- `lag()`, `row_number()`
- Rolling aggregations: 7-day and 30-day spend, transaction counts
- Velocity features: rapid-succession detection, customer transaction sequencing
</details>

<details>
<summary><strong>Delta Lake</strong></summary>

- ACID transactions and the `_delta_log/` transaction log
- `OPTIMIZE` and `ZORDER BY` for query performance
- `VACUUM` for storage management
- `MERGE` (upserts) for incremental processing
- `DESCRIBE HISTORY` for transaction-level audit trails
- Watermark-based incremental loads using a control table
</details>

<details>
<summary><strong>Databricks-Specific</strong></summary>

- Unity Catalog 3-level namespace (`catalog.schema.table`)
- Managed Volumes for file storage
- Serverless compute
- Databricks Workflows for multi-task orchestration
- Databricks Asset Bundles for declarative deployment
</details>

---

## 🏦 Australian Regulatory Context

This pipeline incorporates data engineering patterns relevant to Australian financial services compliance:

| Regulation | Relevance to This Pipeline |
|------------|---------------------------|
| **AUSTRAC AML/CTF Act 2006** | Transaction monitoring, $10,000 threshold reporting, structuring detection |
| **APRA CPS 234** | Data integrity checks, audit columns, pipeline lineage |
| **AFCA Fraud Categories** | Fraud pattern taxonomy used in feature engineering |
| **CDR (Consumer Data Right)** | Schema standards for customer and transaction data |
| **Privacy Act 1988** | No real customer data used — all records are synthetic |

---

## 📁 Project Structure

```
finguard-fraud-pipeline/
│
├── 🐍 data_generator/
│   ├── generate_data.py                   # Synthetic AU banking data generator
│   └── requirements.txt                   # Local Python dependencies
|
├── 📄 docs/
│   └── databricks_setup_guide.md          # Step-by-step Databricks setup
|
├── 📓 notebooks/
|   ├── 00_environment_setup.ipynb         # catalog, schema, watermark table creation
│   ├── 01_bronze_ingestion.ipynb          # Raw ingestion → Delta Bronze
│   ├── 02_silver_transformation.ipynb     # Cleaning, joins, window functions
│   └── 03_gold_feature_engineering.ipynb  # Fraud features, ML output
│
├── resources/
│   └── finguard_pipeline_workflow.job.yml   # Workflow definition (Asset Bundle)
│
├── databricks.yml                           # Asset Bundle root config
├── .gitignore
└── README.md
```

---

## 🤝 Contributing

This is a portfolio project. Issues and suggestions welcome.

---

<div align="center">

**Built to demonstrate production-grade data engineering skills**  
**with PySpark and Databricks, modelled on Australian financial services.**

</div>