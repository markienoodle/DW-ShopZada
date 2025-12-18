import os
from sqlalchemy import create_engine, text
import pandas as pd

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
#                 TABLE CONFIG
# =========================================================
STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "user_credit_card_cleaned"

REQUIRED_COLUMNS = [
    "user_id",
    "name",
    "issuing_bank"
]

# =========================================================
#                   VERIFICATION
# =========================================================
def verify_table_exists(engine):
    query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table
        );
    """)
    exists = engine.execute(
        query, {"schema": STAGING_SCHEMA, "table": STAGING_TABLE}
    ).scalar()

    if not exists:
        raise RuntimeError("❌ Staging table does NOT exist")

    print("✅ Table exists")


def verify_columns(engine):
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name = :table;
    """)

    result = engine.execute(
        query, {"schema": STAGING_SCHEMA, "table": STAGING_TABLE}
    )

    columns = {row[0] for row in result}
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]

    if missing:
        raise RuntimeError(f"❌ Missing columns: {missing}")

    print("✅ All required columns present")


def verify_row_count(engine):
    count = engine.execute(
        text(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{STAGING_TABLE}")
    ).scalar()

    print("\n📊 ROW COUNT")
    print(f"  Total rows: {count}")

    if count == 0:
        raise RuntimeError("❌ Table is empty")


def verify_nulls(engine):
    print("\n🔍 NULL VALUE CHECK")

    for col in REQUIRED_COLUMNS:
        nulls = engine.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                WHERE {col} IS NULL
                """
            )
        ).scalar()

        print(f"  {col}: {nulls} NULL(s)")


def verify_duplicates(engine):
    print("\n🔁 DUPLICATE CHECK (COUNT ONLY)")

    dup_count = engine.execute(
        text(
            f"""
            SELECT COUNT(*) FROM (
                SELECT user_id
                FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                GROUP BY user_id
                HAVING COUNT(*) > 1
            ) t
            """
        )
    ).scalar()

    print(f"  Duplicate ['user_id']: {dup_count}")

    if dup_count > 0:
        print("  ⚠️  Duplicates detected (not blocked)")
    else:
        print("  ✅ No duplicates found")


def verify_bank_format(engine):
    print("\n🏦 BANK NAME FORMAT CHECK")

    invalid = engine.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM {STAGING_SCHEMA}.{STAGING_TABLE}
            WHERE issuing_bank ~ '[0-9]'
            """
        )
    ).scalar()

    if invalid > 0:
        print(f"  ⚠️  {invalid} bank names contain numbers")
    else:
        print("  ✅ Bank names look valid")


# =========================================================
#                   MAIN
# =========================================================
def main():
    print("=" * 60)
    print("🔍 USER CREDIT CARD TABLE VERIFICATION")
    print("=" * 60)

    engine = create_engine(DB_URI)

    verify_table_exists(engine)
    verify_columns(engine)
    verify_row_count(engine)
    verify_nulls(engine)
    verify_duplicates(engine)
    verify_bank_format(engine)

    print("\n✅ VERIFICATION COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    main()