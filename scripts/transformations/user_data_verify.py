import os
import pandas as pd
from sqlalchemy import create_engine

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Airflow container override
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
SOURCE_TABLE = "raw_schema.user_data_json"
STAGING_TABLE = "staging1_schema.user_data_cleaned"

REQUIRED_COLUMNS = [
    "user_id",
    "creation_date",
    "name",
    "street",
    "state",
    "city",
    "country",
    "birthdate",
    "gender",
    "device_address",
    "user_type"
]

# =========================================================
#                VERIFICATION FUNCTIONS
# =========================================================
def verify_columns(df):
    return [c for c in REQUIRED_COLUMNS if c not in df.columns]


def verify_country(df):
    return df[df["country"] != "United States"].shape[0]


def verify_nulls(df):
    return df[REQUIRED_COLUMNS].isna().sum()


def verify_dates(df):
    creation_nulls = df["creation_date"].isna().sum()
    birthdate_nulls = df["birthdate"].isna().sum()
    return creation_nulls, birthdate_nulls


def count_duplicate_ids(df):
    return df["user_id"].duplicated().sum()


def verify_state_coverage(df):
    total = len(df)
    with_state = df["state"].notna().sum()
    missing_state = total - with_state
    return total, with_state, missing_state

# =========================================================
#                MAIN VERIFICATION
# =========================================================
def main():
    engine = create_engine(DB_URI)

    print("📥 Loading raw data...")
    raw_df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", engine)

    print("📥 Loading cleaned data...")
    clean_df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", engine)

    print("\n" + "=" * 70)
    print("🔍 USER DATA VERIFICATION REPORT")
    print("=" * 70)

    # ---- Column validation
    missing_cols = verify_columns(clean_df)
    if missing_cols:
        print(f"❌ Missing required columns: {missing_cols}")
    else:
        print("✅ Required columns present")

    # ---- Country enforcement
    bad_country = verify_country(clean_df)
    print(
        "🇺🇸 Country enforcement:",
        "PASS" if bad_country == 0 else f"FAIL ❌ {bad_country} invalid rows"
    )

    # ---- Null analysis
    print("\n📊 NULL COUNTS")
    print(verify_nulls(clean_df))

    # ---- Date validation
    bad_creation, bad_birthdate = verify_dates(clean_df)
    print("\n📅 Date validation")
    print(f"  creation_date nulls: {bad_creation}")
    print(f"  birthdate nulls: {bad_birthdate}")

    # ---- Duplicate counting ONLY (no failure)
    dup_count = count_duplicate_ids(clean_df)
    print("\n🆔 Duplicate user_id count (info only):")
    print(f"  Duplicate rows: {dup_count}")

    # ---- State coverage
    total, with_state, without_state = verify_state_coverage(clean_df)
    print("\n🗺️ State coverage")
    print(f"  Total records: {total}")
    print(f"  With state: {with_state}")
    print(f"  Missing state: {without_state}")

    print("\n" + "=" * 70)

    # =====================================================
    # FINAL VERDICT (duplicates DO NOT fail)
    # =====================================================
    if missing_cols or bad_country > 0:
        raise RuntimeError("❌ DATA VERIFICATION FAILED — Critical issues found")
    else:
        print("🎉 DATA VERIFICATION PASSED — Ready for downstream models!")

if __name__ == "__main__":
    main()