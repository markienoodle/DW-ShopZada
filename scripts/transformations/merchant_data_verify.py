import os
import pandas as pd
from sqlalchemy import create_engine, text

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
SCHEMA = "staging1_schema"
TABLE = "merchant_data_cleaned"

REQUIRED_COLUMNS = [
    "merchant_id",
    "creation_date",
    "name",
    "street",
    "state",
    "city",
    "country",
    "contact_number"
]

EXPECTED_SQL_TYPES = {
    "merchant_id": "character varying",
    "creation_date": "date",
    "name": "character varying",
    "street": "character varying",
    "state": "character varying",
    "city": "character varying",
    "country": "character varying",
    "contact_number": "character varying"
}

# =========================================================
#              VERIFICATION FUNCTIONS
# =========================================================
def assert_table_exists(engine):
    query = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = :schema
      AND table_name = :table
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {"schema": SCHEMA, "table": TABLE}
        ).fetchone()

    if not result:
        raise AssertionError(f"❌ Table {SCHEMA}.{TABLE} does NOT exist")

    print("✅ Table exists")


def assert_columns(engine):
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema
      AND table_name = :table
    """
    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text(query),
            {"schema": SCHEMA, "table": TABLE}
        )]

    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise AssertionError(f"❌ Missing columns: {missing}")

    print("✅ All required columns present")


def assert_column_types(engine):
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = :schema
      AND table_name = :table
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(query),
            {"schema": SCHEMA, "table": TABLE}
        ).fetchall()

    mismatches = []
    for col, dtype in rows:
        expected = EXPECTED_SQL_TYPES.get(col)
        if expected and expected not in dtype:
            mismatches.append((col, dtype, expected))

    if mismatches:
        raise AssertionError(
            "❌ Column type mismatches:\n" +
            "\n".join([f"{c}: {a} ≠ {e}" for c, a, e in mismatches])
        )

    print("✅ Column data types valid")


def assert_row_count(engine):
    df = pd.read_sql(
        f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.{TABLE}",
        engine
    )
    if df.loc[0, "cnt"] == 0:
        raise AssertionError("❌ Table is empty")

    print(f"✅ Row count: {df.loc[0, 'cnt']}")


def assert_business_rules(engine):
    df = pd.read_sql(
        f"SELECT * FROM {SCHEMA}.{TABLE}",
        engine
    )

    # merchant_id not null
    if df["merchant_id"].isna().any():
        raise AssertionError("❌ merchant_id contains NULLs")

    # contact_number digits only
    bad_contacts = df[
        df["contact_number"].notna() &
        ~df["contact_number"].str.match(r"^\d+$")
    ]
    if not bad_contacts.empty:
        raise AssertionError("❌ contact_number contains non-digit values")

    # country always United States
    if not (df["country"].dropna() == "United States").all():
        raise AssertionError("❌ country contains non-US values")

    print("✅ Business rules validated")


def assert_geocoding_quality(engine, max_missing_pct=0.3):
    df = pd.read_sql(
        f"SELECT state FROM {SCHEMA}.{TABLE}",
        engine
    )

    missing_ratio = df["state"].isna().mean()
    print(f"📍 Missing state ratio: {missing_ratio:.2%}")

    if missing_ratio > max_missing_pct:
        raise AssertionError(
            f"❌ Too many missing states ({missing_ratio:.2%})"
        )

    print("✅ Geocoding quality acceptable")


def assert_no_duplicate_merchants(engine):
    query = f"""
    SELECT merchant_id, COUNT(*)
    FROM {SCHEMA}.{TABLE}
    GROUP BY merchant_id
    HAVING COUNT(*) > 1
    """
    df = pd.read_sql(query, engine)

    if not df.empty:
        raise AssertionError(
            f"❌ Duplicate merchant_id found:\n{df.head()}"
        )

    print("✅ No duplicate merchant_id values")


# =========================================================
#                       MAIN
# =========================================================
def main():
    print("\n" + "="*60)
    print("🔍 VERIFYING merchant_data_cleaned TRANSFORMATION")
    print("="*60)

    engine = create_engine(DB_URI)

    assert_table_exists(engine)
    assert_row_count(engine)
    assert_columns(engine)
    assert_column_types(engine)
    assert_business_rules(engine)
    assert_geocoding_quality(engine)
    assert_no_duplicate_merchants(engine)

    print("\n🎉 ALL VERIFICATION CHECKS PASSED\n")


if __name__ == "__main__":
    main()