import os
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

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

TABLE_SCHEMA = "staging1_schema"
TABLE_NAME = "product_list_cleaned"

# Expected schema: for numeric types, include precision & scale
EXPECTED_SCHEMA = [
    ("product_id", "character varying", 12, None, None),
    ("product_name", "character varying", 100, None, None),
    ("product_type", "character varying", 50, None, None),
    ("price", "numeric", None, 5, 2),
]

# =========================================================
#                     VERIFICATION
# =========================================================
def main():
    engine = create_engine(DATABASE_URL)

    with engine.begin() as conn:

        # 1. Table existence
        exists = conn.execute(text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table
        """), {"schema": TABLE_SCHEMA, "table": TABLE_NAME}).fetchone()

        if not exists:
            raise RuntimeError("❌ Staging table does not exist")

        # 2. Column structure validation (order + type)
        columns = conn.execute(text("""
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
        """), {"schema": TABLE_SCHEMA, "table": TABLE_NAME}).fetchall()

        if len(columns) != len(EXPECTED_SCHEMA):
            raise RuntimeError("❌ Column count mismatch")

        for actual, expected in zip(columns, EXPECTED_SCHEMA):
            col_name, data_type, char_len, num_prec, num_scale = actual
            exp_name, exp_type, exp_char_len, exp_prec, exp_scale = expected

            if col_name != exp_name or data_type != exp_type:
                raise RuntimeError(
                    f"❌ Schema mismatch for column {col_name}: expected {exp_type}, got {data_type}"
                )

            if data_type == "character varying" and char_len != exp_char_len:
                raise RuntimeError(
                    f"❌ Length mismatch for {col_name}: expected {exp_char_len}, got {char_len}"
                )

            if data_type == "numeric":
                if num_prec != exp_prec or num_scale != exp_scale:
                    raise RuntimeError(
                        f"❌ Numeric mismatch for {col_name}: expected ({exp_prec},{exp_scale}), got ({num_prec},{num_scale})"
                    )

        # 3. Row count sanity
        row_count = conn.execute(text(f"""
            SELECT COUNT(*) FROM {TABLE_SCHEMA}.{TABLE_NAME}
        """)).scalar()

        if row_count == 0:
            raise RuntimeError("❌ Staging table is empty")

        # 4. Price validation (no negatives)
        bad_price = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM {TABLE_SCHEMA}.{TABLE_NAME}
            WHERE price < 0
        """)).scalar()

        if bad_price > 0:
            raise RuntimeError("❌ Negative price values detected")

    print("✅ Staging verification passed")

if __name__ == "__main__":
    main()
