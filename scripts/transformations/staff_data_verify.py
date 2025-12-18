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
TABLE = "staff_data_cleaned"

REQUIRED_COLUMNS = [
    "staff_id",
    "name",
    "job_level",
    "street",
    "state",
    "city",
    "country",
    "contact_number",
    "creation_date"
]

EXPECTED_SQL_TYPES = {
    "staff_id": "character varying",
    "name": "character varying",
    "job_level": "character varying",
    "street": "character varying",
    "state": "character varying",
    "city": "character varying",
    "country": "character varying",
    "contact_number": "character varying",
    "creation_date": "date"
}

# =========================================================
#              VERIFICATION FUNCTIONS
# =========================================================
def assert_table_exists(engine):
    q = """
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        if not conn.execute(text(q), {"schema": SCHEMA, "table": TABLE}).fetchone():
            raise AssertionError(f"❌ Table {SCHEMA}.{TABLE} does not exist")
    print("✅ Table exists")


def assert_row_count(engine):
    df = pd.read_sql(f"SELECT COUNT(*) cnt FROM {SCHEMA}.{TABLE}", engine)
    if df.loc[0, "cnt"] == 0:
        raise AssertionError("❌ Table is empty")
    print(f"✅ Row count: {df.loc[0, 'cnt']}")


def assert_columns(engine):
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(text(q), {"schema": SCHEMA, "table": TABLE})]

    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise AssertionError(f"❌ Missing columns: {missing}")

    print("✅ All required columns present")


def assert_column_types(engine):
    q = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        rows = conn.execute(text(q), {"schema": SCHEMA, "table": TABLE}).fetchall()

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


def assert_business_rules(engine):
    df = pd.read_sql(f"SELECT * FROM {SCHEMA}.{TABLE}", engine)

    if df["staff_id"].isna().any():
        raise AssertionError("❌ staff_id contains NULLs")

    bad_contacts = df[
        df["contact_number"].notna() &
        ~df["contact_number"].str.match(r"^\d+$")
    ]
    if not bad_contacts.empty:
        raise AssertionError("❌ contact_number contains non-digit values")

    if not (df["country"].dropna() == "United States").all():
        raise AssertionError("❌ country contains non-US values")

    print("✅ Business rules validated")


def assert_geocoding_quality(engine, max_missing_pct=0.30):
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


def assert_no_duplicate_staff(engine):
    q = f"""
    SELECT staff_id, COUNT(*)
    FROM {SCHEMA}.{TABLE}
    GROUP BY staff_id
    HAVING COUNT(*) > 1
    """
    df = pd.read_sql(q, engine)

    if not df.empty:
        raise AssertionError(
            f"❌ Duplicate staff_id found:\n{df.head()}"
        )

    print("✅ No duplicate staff_id values")


# =========================================================
#                       MAIN
# =========================================================
def main():
    print("\n" + "="*60)
    print("🔍 VERIFYING staff_data_cleaned TRANSFORMATION")
    print("="*60)

    engine = create_engine(DB_URI)

    assert_table_exists(engine)
    assert_row_count(engine)
    assert_columns(engine)
    assert_column_types(engine)
    assert_business_rules(engine)
    assert_geocoding_quality(engine)
    assert_no_duplicate_staff(engine)

    print("\n🎉 ALL STAFF DATA VERIFICATION CHECKS PASSED\n")


if __name__ == "__main__":
    main()