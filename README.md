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

FinGuard is a real-world financial data engineering pipeline that ingests, transforms, and feature-engineers **500,000 synthetic Australian banking transactions** to support fraud detection at scale.

The pipeline is built using the **Medallion Architecture** (Bronze → Silver → Gold) on Databricks with Delta Lake as the storage layer and Unity Catalog for governance — the same stack used by Australian banks on their cloud data platforms.

### Why This Project?

Australian banks process over **$1 trillion in card transactions annually** (RBA Payments Data). Fraud detection pipelines at CBA, NAB, Westpac, and ANZ rely on exactly the patterns demonstrated here: scalable ingestion, enriched feature tables, and low-latency risk scoring using Delta Lake and Spark.

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
║  • Explicit StructType schema enforcement                                ║
║  • PERMISSIVE ingestion with dead-letter quarantine                      ║
║  • Audit columns: _ingested_at, _batch_id, _source_file                  ║
║  • Partitioned by txn_month for query pruning                            ║
║  • OPTIMIZE + ZORDER BY (customer_id, txn_date)                          ║
║  • Data quality checks (APRA CPS 234 aligned)                            ║
╚══════════════════════════════════════════════════════════════════════════╝
                               │
                               ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                    🥈  SILVER LAYER  (finguard.silver.*)                 ║
║                                                                          ║
║  • Type casting & data standardisation                                   ║
║  • Deduplication (transaction_id idempotency)                            ║
║  • Broadcast joins: transactions ↔ customers ↔ merchants                 ║
║  • Window functions: rolling 7/30-day spend averages                     ║
║  • SCD Type 2 for customer dimension                                     ║
║  • Bad row quarantine to finguard.monitoring.dead_letter                 ║
╚══════════════════════════════════════════════════════════════════════════╝
                               │
                               ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                    🥇  GOLD LAYER  (finguard.gold.*)                     ║
║                                                                          ║
║  • Fraud feature engineering (AUSTRAC typology aligned)                  ║
║  • Customer risk scoring with rolling velocity features                  ║
║  • Delta MERGE (upserts) for incremental processing                      ║
║  • ML-ready feature table for model training                             ║
║  • Per-merchant risk profiles                                            ║
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
│   ├── transactions               ← Delta table (partitioned by txn_month)
│   ├── customers                  ← Delta table (partitioned by address_state)
│   └── merchants                  ← Delta table
├── silver/                        ← Schema
│   ├── transactions_cleaned       ← Delta table
│   └── transactions_enriched      ← Delta table (joined with customers + merchants)
├── gold/                          ← Schema
│   ├── fraud_features             ← Delta table (ML-ready)
│   └── customer_risk_scores       ← Delta table (upserted via MERGE)
└── monitoring/                    ← Schema
    ├── dq_results                 ← Data quality metrics over time
    └── dead_letter                ← Quarantined bad rows
```

---

## 📓 Notebooks

| Notebook | Layer | Status | Key PySpark Concepts |
|----------|-------|--------|---------------------|
| `01_bronze_ingestion.ipynb` | Bronze | ✅ Complete | StructType schemas, Delta writes, partitioning, OPTIMIZE, ZORDER, time travel, DQ checks, AQE |
| `02_silver_transformation.ipynb` | Silver | 🔄 In Progress | Joins, window functions, deduplication, SCD Type 2, dead-letter tables |
| `03_gold_feature_engineering.ipynb` | Gold | 🔄 In Progress | Fraud features, UDFs, Delta MERGE, ML feature table |

---

## ⚙️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Processing Engine | Apache Spark 3.5 (PySpark) | Distributed data processing |
| Platform | Databricks Runtime 14.3 LTS | Managed Spark + Delta |
| Storage Format | Delta Lake 3.0 | ACID tables with time travel |
| Data Governance | Unity Catalog | 3-level namespace, access control |
| Language | Python 3.10 | PEP 8 compliant throughout |
| Version Control | Git + Databricks Repos | CI/CD-ready notebook versioning |

---

## 🚀 Quickstart

### Prerequisites
- [Databricks Free Edition](https://community.cloud.databricks.com) account
- Python 3.8+ with pip

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

Output:
```
❶  Generating customers...
    ✓  data/raw/customers/customers.csv  (1.2 MB  |  5,000 rows)
❷  Generating merchants...
    ✓  data/raw/merchants/merchants.csv  (120 KB  |  800 rows)
❸  Generating transactions...
    ... 100,000 / 500,000  (20%)
    ... 200,000 / 500,000  (40%)
    ...
    ✓  data/raw/transactions/transactions.csv  (118.4 MB  |  500,000 rows)
    ✓  Fraud rate: 3.87%
```

### 3. Create Unity Catalog structure in Databricks

```sql
-- Run in a Databricks notebook
CREATE CATALOG IF NOT EXISTS finguard;
CREATE SCHEMA IF NOT EXISTS finguard.raw;
CREATE VOLUME IF NOT EXISTS finguard.raw.source_files;
```

### 4. Upload CSVs to the Volume

Via Databricks UI: **Catalog → finguard → raw → source_files → Upload**

Or via CLI:
```bash
pip install databricks-cli
databricks configure --token
databricks fs cp data/raw/transactions/transactions.csv /Volumes/finguard/raw/source_files/
databricks fs cp data/raw/customers/customers.csv      /Volumes/finguard/raw/source_files/
databricks fs cp data/raw/merchants/merchants.csv      /Volumes/finguard/raw/source_files/
```

### 5. Run notebooks in order

```
notebooks/01_bronze_ingestion.ipynb
notebooks/02_silver_transformation.ipynb    ← coming soon
notebooks/03_gold_feature_engineering.ipynb ← coming soon
```

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
- Why `inferSchema=False` in production
- Type casting in Silver layer
- Schema evolution with `autoMerge`
</details>

<details>
<summary><strong>Joins & Performance</strong></summary>

- Broadcast joins for small lookup tables (`F.broadcast()`)
- Sort-merge joins for large tables
- `repartition` vs `coalesce` — when to use each
- Partition pruning via `partitionBy`
</details>

<details>
<summary><strong>Window Functions</strong></summary>

- `Window.partitionBy().orderBy()`
- `lag()`, `lead()`, `row_number()`, `rank()`
- Rolling aggregations: 7-day and 30-day spend averages
- Velocity features: transactions per hour per customer
</details>

<details>
<summary><strong>Delta Lake</strong></summary>

- ACID transactions and the `_delta_log/` transaction log
- Time travel: `versionAsOf` and `timestampAsOf`
- `OPTIMIZE` and `ZORDER BY` for query performance
- `VACUUM` for storage management
- `MERGE` (upserts) for incremental processing
- `DESCRIBE HISTORY` for audit trails
</details>

<details>
<summary><strong>Databricks-Specific</strong></summary>

- Unity Catalog 3-level namespace (`catalog.schema.table`)
- Managed Volumes for file storage
- Serverless compute configuration
- AQE (Adaptive Query Execution)
- `autoCompact` and `optimizeWrite`
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
| **Privacy Act 1988** | PII handling — no real customer data used |

---

## 📁 Project Structure

```
finguard-fraud-pipeline/
│
├── 📓 notebooks/
│   ├── 01_bronze_ingestion.ipynb          # Raw ingestion → Delta Bronze
│   ├── 02_silver_transformation.ipynb     # Cleaning, joins, window functions
│   └── 03_gold_feature_engineering.ipynb  # Fraud features, ML output
│
├── 🐍 data_generator/
│   ├── generate_data.py                   # Synthetic AU banking data generator
│   └── requirements.txt                   # Local Python dependencies
│
├── 📄 docs/
│   └── databricks_setup_guide.md          # Step-by-step Databricks setup
│
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
