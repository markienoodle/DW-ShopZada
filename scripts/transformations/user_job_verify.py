import os
import pandas as pd
from sqlalchemy import create_engine

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "shopzada_dwh")

# Detect Airflow container
if os.path.exists("/opt/airflow"):
    DB_HOST = "shopzada-postgres-db"

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 VERIFICATION CONFIG
# =========================================================
STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "user_job_cleaned"

REQUIRED_COLUMNS = [
    "user_id",
    "name",
    "job_title",
    "job_level"
]

PRIMARY_KEY = ["user_id"]

# =========================================================
#                 VERIFICATION FUNCTIONS
# =========================================================
def verify_table_exists(engine):
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{STAGING_SCHEMA}'
            AND table_name = '{STAGING_TABLE}'
        );
    """
    exists = engine.execute(query).scalar()
    if not exists:
        raise RuntimeError(f"❌ Table {STAGING_SCHEMA}.{STAGING_TABLE} does not exist")
    print("✅ Table exists")


def verify_columns(df):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"❌ Missing required columns: {missing}")
    print("✅ All required columns present")


def verify_nulls(df):
    print("\n🔍 NULL VALUE CHECK")
    for col in REQUIRED_COLUMNS:
        null_count = df[col].isna().sum()
        print(f"  {col}: {null_count} NULL(s)")


def verify_duplicate_count(df):
    print("\n🔁 DUPLICATE CHECK (COUNT ONLY)")
    dup_count = df.duplicated(subset=PRIMARY_KEY).sum()
    print(f"  Duplicate {PRIMARY_KEY}: {dup_count}")

    if dup_count > 0:
        print("  ⚠️  Duplicates detected (not blocked)")
    else:
        print("  ✅ No duplicates found")


def verify_row_count(df):
    print("\n📊 ROW COUNT")
    print(f"  Total rows: {len(df)}")

# =========================================================
#                 MAIN VERIFICATION
# =========================================================
def main():
    engine = create_engine(DB_URI)

    print("\n" + "=" * 60)
    print("🔎 USER JOB DATA VERIFICATION")
    print("=" * 60)

    verify_table_exists(engine)

    df = pd.read_sql(
        f"SELECT * FROM {STAGING_SCHEMA}.{STAGING_TABLE}",
        engine
    )

    verify_columns(df)
    verify_row_count(df)
    verify_nulls(df)
    verify_duplicate_count(df)

    print("\n✅ VERIFICATION COMPLETED SUCCESSFULLY")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()