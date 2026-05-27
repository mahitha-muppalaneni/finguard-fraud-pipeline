"""
FinGuard Fraud Detection Pipeline
==================================
Module  : data_generator/generate_data.py
Purpose : Generate synthetic Australian banking data for pipeline development
Author  : FinGuard Pipeline Project
Version : 1.0.0

Generates realistic transaction, customer, and merchant data modelled after
Australian retail banking patterns (CBA / NAB / Westpac style).

Fraud patterns injected (AUSTRAC typology aligned):
    - Card-Not-Present (CNP) fraud         ~1.5%
    - Velocity attacks (rapid successive)  ~0.5%
    - Geographic anomalies                 ~0.4%
    - Account takeover (ATO)               ~0.3%
    - Structuring / smurfing               ~0.2%

Usage:
    pip install -r requirements.txt
    python generate_data.py
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ── Reproducibility ───────────────────────────────────────────────────────────
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
fake = Faker("en_AU")
Faker.seed(RANDOM_SEED)

# ── Volume ────────────────────────────────────────────────────────────────────
N_CUSTOMERS    = 5_000
N_MERCHANTS    = 800
N_TRANSACTIONS = 500_000

# ── Output path ───────────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# ── Australian state distribution (ABS 2023 population weights) ───────────────
AU_STATE_WEIGHTS = {
    "NSW": 0.32,
    "VIC": 0.26,
    "QLD": 0.20,
    "WA":  0.10,
    "SA":  0.07,
    "TAS": 0.02,
    "ACT": 0.02,
    "NT":  0.01,
}

# ── Merchant category profiles (AS/NZS MCC aligned) ──────────────────────────
MCC_PROFILES = {
    "supermarkets_grocery":   {"mcc": "5411", "avg_amount": 95,  "std": 55,  "weight": 0.22},
    "petrol_service_station": {"mcc": "5541", "avg_amount": 85,  "std": 25,  "weight": 0.10},
    "restaurants_cafes":      {"mcc": "5812", "avg_amount": 42,  "std": 28,  "weight": 0.14},
    "online_retail":          {"mcc": "5999", "avg_amount": 130, "std": 110, "weight": 0.12},
    "pharmacies_chemist":     {"mcc": "5912", "avg_amount": 35,  "std": 22,  "weight": 0.06},
    "utilities_telco":        {"mcc": "4900", "avg_amount": 180, "std": 60,  "weight": 0.05},
    "transport_rideshare":    {"mcc": "4111", "avg_amount": 28,  "std": 18,  "weight": 0.08},
    "entertainment":          {"mcc": "7832", "avg_amount": 55,  "std": 30,  "weight": 0.04},
    "healthcare_medical":     {"mcc": "8099", "avg_amount": 90,  "std": 70,  "weight": 0.04},
    "electronics_tech":       {"mcc": "5734", "avg_amount": 450, "std": 380, "weight": 0.03},
    "gambling_tab":           {"mcc": "7995", "avg_amount": 120, "std": 95,  "weight": 0.03},
    "jewellery_luxury":       {"mcc": "5944", "avg_amount": 900, "std": 800, "weight": 0.01},
    "international_transfer": {"mcc": "6099", "avg_amount": 650, "std": 400, "weight": 0.02},
    "atm_withdrawal":         {"mcc": "6011", "avg_amount": 200, "std": 100, "weight": 0.06},
}

PAYMENT_CHANNELS = [
    "card_present",
    "card_not_present",
    "tap_to_pay",
    "bpay",
    "osko",
    "eftpos",
]

CARD_NETWORKS = ["Visa", "Mastercard", "Eftpos"]

HIGH_RISK_CATEGORIES = ["electronics_tech", "jewellery_luxury", "gambling_tab"]


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_hourly_spend_weight(hour: int) -> float:
    """Return a spend-probability weight for a given hour of day.

    Curve modelled on Australian retail banking transaction volumes.
    Peak hours: lunch (12-13) and after-work (16-18).
    """
    hourly_weights = {
        0: 0.3, 1: 0.2, 2: 0.15, 3: 0.1, 4: 0.1, 5: 0.2,
        6: 0.6, 7: 1.2, 8: 2.5, 9: 3.5, 10: 4.0, 11: 4.5,
        12: 5.5, 13: 4.8, 14: 4.2, 15: 4.5, 16: 5.0, 17: 5.5,
        18: 4.8, 19: 4.0, 20: 3.2, 21: 2.5, 22: 1.5, 23: 0.8,
    }
    return hourly_weights.get(hour, 1.0)


def _generate_transaction_amount(category: str) -> float:
    """Generate a realistic transaction amount using log-normal distribution.

    Log-normal reflects real spend: most transactions are small,
    with a long tail of large purchases.
    """
    profile = MCC_PROFILES.get(category, MCC_PROFILES["supermarkets_grocery"])
    amount = np.random.lognormal(
        mean=np.log(max(profile["avg_amount"], 1)),
        sigma=0.6,
    )
    return round(float(np.clip(amount, 0.50, 50_000)), 2)


def _get_payment_channel(category: str) -> str:
    """Return a payment channel weighted by merchant category."""
    if category == "atm_withdrawal":
        return "eftpos"
    if category in ("online_retail", "international_transfer"):
        return random.choices(
            ["card_not_present", "osko", "bpay"],
            weights=[0.70, 0.20, 0.10],
        )[0]
    return random.choices(
        PAYMENT_CHANNELS,
        weights=[0.35, 0.15, 0.30, 0.08, 0.07, 0.05],
    )[0]


def _write_csv(df: pd.DataFrame, subfolder: str, filename: str) -> None:
    """Write a DataFrame to CSV and print a confirmation with file size."""
    path = os.path.join(OUTPUT_DIR, subfolder, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"    ✓  {path}  ({size_mb:.1f} MB  |  {len(df):,} rows)")


def _write_schema(df: pd.DataFrame, subfolder: str, filename: str) -> None:
    """Write a JSON schema file alongside the CSV for documentation."""
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    path = os.path.join(OUTPUT_DIR, subfolder, filename)
    with open(path, "w") as fh:
        json.dump(schema, fh, indent=2)
    print(f"    ✓  Schema written: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Public generators
# ─────────────────────────────────────────────────────────────────────────────

def generate_customers(n: int) -> pd.DataFrame:
    """Generate synthetic Australian retail banking customer records.

    Args:
        n: Number of customer records to generate.

    Returns:
        DataFrame with customer demographic and account data.
    """
    print(f"  Generating {n:,} customers...")
    states  = list(AU_STATE_WEIGHTS.keys())
    weights = list(AU_STATE_WEIGHTS.values())
    records = []

    for _ in range(n):
        state = random.choices(states, weights=weights)[0]
        dob   = fake.date_of_birth(minimum_age=18, maximum_age=80)
        age   = (datetime.today().date() - dob).days // 365

        # Income distribution by age bracket (ABS household income data)
        if age < 25:
            income = np.random.normal(45_000, 12_000)
        elif age < 45:
            income = np.random.normal(92_000, 35_000)
        elif age < 60:
            income = np.random.normal(110_000, 45_000)
        else:
            income = np.random.normal(65_000, 28_000)

        income = max(18_000, round(income / 1_000) * 1_000)

        records.append({
            "customer_id":         str(uuid.uuid4()),
            "first_name":          fake.first_name(),
            "last_name":           fake.last_name(),
            "date_of_birth":       dob.isoformat(),
            "gender":              random.choice(["M", "F", "NB", "U"]),
            "email":               fake.ascii_email(),
            "phone":               fake.phone_number(),
            "address_street":      fake.street_address(),
            "address_suburb":      fake.city(),
            "address_state":       state,
            "address_postcode":    fake.postcode(),
            "annual_income_aud":   income,
            "employment_status":   random.choices(
                ["full_time", "part_time", "self_employed",
                 "retired", "student", "unemployed"],
                weights=[0.50, 0.18, 0.12, 0.10, 0.06, 0.04],
            )[0],
            "credit_score":        int(np.clip(np.random.normal(680, 90), 300, 850)),
            "account_open_date":   fake.date_between(
                start_date="-10y", end_date="-1m"
            ).isoformat(),
            "is_high_risk":        random.random() < 0.03,
            "kyc_verified":        random.random() < 0.95,
        })

    df = pd.DataFrame(records)
    print(f"    ✓  {len(df):,} customers generated")
    return df


def generate_merchants(n: int) -> pd.DataFrame:
    """Generate synthetic Australian merchant records.

    Args:
        n: Number of merchant records to generate.

    Returns:
        DataFrame with merchant details, MCC codes, and risk levels.
    """
    print(f"  Generating {n:,} merchants...")
    categories  = list(MCC_PROFILES.keys())
    cat_weights = [MCC_PROFILES[c]["weight"] for c in categories]
    states      = list(AU_STATE_WEIGHTS.keys())
    state_wts   = list(AU_STATE_WEIGHTS.values())
    records     = []

    for _ in range(n):
        category   = random.choices(categories, weights=cat_weights)[0]
        profile    = MCC_PROFILES[category]
        state      = random.choices(states, weights=state_wts)[0]
        is_intl    = random.random() < 0.08

        records.append({
            "merchant_id":      str(uuid.uuid4()),
            "merchant_name":    fake.company(),
            "category":         category,
            "mcc_code":         profile["mcc"],
            "abn":              fake.numerify("## ### ### ###") if not is_intl else None,
            "address_suburb":   fake.city() if not is_intl else None,
            "address_state":    state if not is_intl else None,
            "country":          "AUS" if not is_intl else random.choice(
                ["USA", "GBR", "NZL", "CHN", "SGP", "IND", "PHL"]
            ),
            "is_international": is_intl,
            "is_online_only":   category == "online_retail" or random.random() < 0.12,
            "risk_level":       random.choices(
                ["low", "medium", "high"],
                weights=[0.70, 0.22, 0.08],
            )[0],
            "registered_date":  fake.date_between(
                start_date="-15y", end_date="-6m"
            ).isoformat(),
        })

    df = pd.DataFrame(records)
    print(f"    ✓  {len(df):,} merchants generated")
    return df


def _inject_fraud_patterns(
    df: pd.DataFrame,
    customers: pd.DataFrame,
    merchants: pd.DataFrame,
) -> pd.DataFrame:
    """Inject realistic Australian fraud patterns into the transaction dataset.

    Patterns are based on AUSTRAC typologies and AFCA complaint categories.

    Args:
        df:        Raw transactions DataFrame.
        customers: Customer reference DataFrame.
        merchants: Merchant reference DataFrame.

    Returns:
        DataFrame with is_fraud, fraud_type, and fraud_indicator populated.
    """
    print("    Injecting fraud patterns (AUSTRAC typology aligned)...")
    df = df.copy()
    total = len(df)

    # ── Pattern 1: Card-Not-Present (CNP) ─────────────────────────────────────
    # High-value online transactions between midnight and 4am
    cnp_mask = (
        (df["channel"] == "card_not_present")
        & (df["amount"] > 300)
        & (df["txn_hour"].between(0, 4))
    )
    cnp_idx = df[cnp_mask].sample(frac=0.40, random_state=1).index
    df.loc[cnp_idx, "is_fraud"]        = True
    df.loc[cnp_idx, "fraud_type"]      = "card_not_present"
    df.loc[cnp_idx, "fraud_indicator"] = "high_amount_late_night_cnp"

    # ── Pattern 2: Velocity Attack ─────────────────────────────────────────────
    # Same customer with ≥3 transactions within 10 minutes
    df["txn_ts_dt"] = pd.to_datetime(df["txn_timestamp"])
    velocity_counts = (
        df.groupby("customer_id")["txn_ts_dt"]
        .apply(lambda s: (s.sort_values().diff().dt.total_seconds() < 600).sum())
    )
    high_velocity_customers = velocity_counts[velocity_counts >= 3].index.tolist()
    velocity_idx = df[
        df["customer_id"].isin(high_velocity_customers) & ~df["is_fraud"]
    ].sample(frac=0.25, random_state=2).index
    df.loc[velocity_idx, "is_fraud"]        = True
    df.loc[velocity_idx, "fraud_type"]      = "velocity_attack"
    df.loc[velocity_idx, "fraud_indicator"] = "rapid_successive_transactions"

    # ── Pattern 3: Geographic Anomaly ─────────────────────────────────────────
    # International merchant transaction while customer is AU-resident
    intl_merchant_ids = merchants[merchants["is_international"]]["merchant_id"].tolist()
    geo_idx = df[
        df["merchant_id"].isin(intl_merchant_ids)
        & ~df["is_fraud"]
        & (df["amount"] > 200)
    ].sample(frac=0.30, random_state=3).index
    df.loc[geo_idx, "is_fraud"]        = True
    df.loc[geo_idx, "fraud_type"]      = "geographic_anomaly"
    df.loc[geo_idx, "fraud_indicator"] = "international_while_domestic_active"

    # ── Pattern 4: Account Takeover (ATO) ─────────────────────────────────────
    # High-risk customers purchasing in premium/fraud-prone categories
    high_risk_customer_ids = customers[customers["is_high_risk"]]["customer_id"].tolist()
    high_risk_merchant_ids = merchants[
        merchants["category"].isin(HIGH_RISK_CATEGORIES)
    ]["merchant_id"].tolist()
    ato_idx = df[
        df["customer_id"].isin(high_risk_customer_ids)
        & df["merchant_id"].isin(high_risk_merchant_ids)
        & ~df["is_fraud"]
    ].sample(frac=0.45, random_state=4).index
    df.loc[ato_idx, "is_fraud"]        = True
    df.loc[ato_idx, "fraud_type"]      = "account_takeover"
    df.loc[ato_idx, "fraud_indicator"] = "high_risk_customer_premium_category"

    # ── Pattern 5: Structuring / Smurfing ─────────────────────────────────────
    # Transactions just under the AUSTRAC $10,000 AUD reporting threshold
    structuring_idx = df[
        df["amount"].between(9_000, 9_999) & ~df["is_fraud"]
    ].sample(frac=0.60, random_state=5).index
    df.loc[structuring_idx, "is_fraud"]        = True
    df.loc[structuring_idx, "fraud_type"]      = "structuring_smurfing"
    df.loc[structuring_idx, "fraud_indicator"] = "just_under_austrac_threshold"

    df.drop(columns=["txn_ts_dt"], inplace=True)

    fraud_count = df["is_fraud"].sum()
    fraud_rate  = fraud_count / total * 100
    print(f"    ✓  {fraud_count:,} fraudulent transactions injected ({fraud_rate:.2f}%)")
    return df


def generate_transactions(
    n: int,
    customers: pd.DataFrame,
    merchants: pd.DataFrame,
) -> pd.DataFrame:
    """Generate synthetic Australian banking transactions.

    Covers one full AU financial year (1 Jul 2023 – 30 Jun 2024).
    Amounts follow log-normal distribution. Hourly volume follows real AU
    retail banking spending curves.

    Args:
        n:         Number of transactions to generate.
        customers: Customer reference DataFrame.
        merchants: Merchant reference DataFrame.

    Returns:
        DataFrame with transactions and injected fraud labels.
    """
    print(f"  Generating {n:,} transactions (~2 minutes)...")

    customer_ids  = customers["customer_id"].tolist()
    merchant_ids  = merchants["merchant_id"].tolist()
    mcc_map       = merchants.set_index("merchant_id")["category"].to_dict()
    categories    = list(MCC_PROFILES.keys())
    cat_weights   = [MCC_PROFILES[c]["weight"] for c in categories]

    # AU financial year: 1 July 2023 → 30 June 2024
    start_date          = datetime(2023, 7, 1)
    end_date            = datetime(2024, 6, 30)
    date_range_seconds  = int((end_date - start_date).total_seconds())

    hours       = list(range(24))
    hr_weights  = [_get_hourly_spend_weight(h) for h in hours]

    records     = []
    log_interval = 50_000

    for i in range(n):
        if i > 0 and i % log_interval == 0:
            print(f"    ... {i:,} / {n:,}  ({i / n * 100:.0f}%)")

        merchant_id = random.choice(merchant_ids)
        category    = mcc_map.get(
            merchant_id,
            random.choices(categories, weights=cat_weights)[0],
        )

        txn_hour   = random.choices(hours, weights=hr_weights)[0]
        rand_secs  = random.randint(0, date_range_seconds)
        txn_ts     = (start_date + timedelta(seconds=rand_secs)).replace(
            hour=txn_hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        response_code = random.choices(
            ["00", "05", "51", "54", "61"],
            weights=[0.96, 0.01, 0.01, 0.01, 0.01],
        )[0]

        is_international = bool(
            merchants.loc[
                merchants["merchant_id"] == merchant_id, "is_international"
            ].values[0]
            if merchant_id in merchants["merchant_id"].values
            else False
        )

        records.append({
            "transaction_id":    str(uuid.uuid4()),
            "customer_id":       random.choice(customer_ids),
            "merchant_id":       merchant_id,
            "txn_timestamp":     txn_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "txn_date":          txn_ts.strftime("%Y-%m-%d"),
            "txn_month":         txn_ts.strftime("%Y-%m"),
            "txn_hour":          txn_hour,
            "txn_day_of_week":   txn_ts.strftime("%A"),
            "amount":            _generate_transaction_amount(category),
            "currency":          "AUD",
            "channel":           _get_payment_channel(category),
            "card_network":      random.choices(
                CARD_NETWORKS,
                weights=[0.45, 0.40, 0.15],
            )[0],
            "merchant_category": category,
            "mcc_code":          MCC_PROFILES.get(category, {}).get("mcc", "9999"),
            "response_code":     response_code,
            "is_declined":       response_code != "00",
            "is_international":  is_international,
            "device_type":       random.choices(
                ["mobile", "desktop", "pos_terminal", "atm", "unknown"],
                weights=[0.38, 0.20, 0.30, 0.07, 0.05],
            )[0],
            "ip_country":        "AUS" if random.random() < 0.88 else random.choice(
                ["USA", "GBR", "NZL", "IND", "CHN"]
            ),
            "is_fraud":          False,
            "fraud_type":        None,
            "fraud_indicator":   None,
        })

    df = pd.DataFrame(records)
    df = _inject_fraud_patterns(df, customers, merchants)

    print(f"    ✓  {len(df):,} transactions generated")
    print(f"    ✓  Fraud rate: {df['is_fraud'].mean() * 100:.2f}%")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Run the full data generation pipeline."""
    print("\n" + "═" * 60)
    print("  FinGuard — Synthetic AU Banking Data Generator")
    print("  Modelled on CBA / NAB / Westpac transaction patterns")
    print("═" * 60 + "\n")

    print("❶  Generating customers...")
    customers = generate_customers(N_CUSTOMERS)
    _write_csv(customers, "customers", "customers.csv")
    _write_schema(customers, "customers", "customers_schema.json")

    print("\n❷  Generating merchants...")
    merchants = generate_merchants(N_MERCHANTS)
    _write_csv(merchants, "merchants", "merchants.csv")
    _write_schema(merchants, "merchants", "merchants_schema.json")

    print("\n❸  Generating transactions...")
    transactions = generate_transactions(N_TRANSACTIONS, customers, merchants)
    _write_csv(transactions, "transactions", "transactions.csv")
    _write_schema(transactions, "transactions", "transactions_schema.json")

    print("\n" + "═" * 60)
    print("  Generation Complete")
    print(f"  Customers    : {len(customers):>10,}")
    print(f"  Merchants    : {len(merchants):>10,}")
    print(f"  Transactions : {len(transactions):>10,}")
    print(
        f"  Fraud txns   : {transactions['is_fraud'].sum():>10,}"
        f"  ({transactions['is_fraud'].mean() * 100:.2f}%)"
    )
    print("═" * 60)
    print("\nNext step → Upload the 3 CSVs to:")
    print("  /Volumes/finguard/raw/source_files/")
    print("Then run: notebooks/01_bronze_ingestion.ipynb\n")


if __name__ == "__main__":
    main()
