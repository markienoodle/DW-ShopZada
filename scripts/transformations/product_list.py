import os
import pandas as pd
from sqlalchemy import MetaData, create_engine
from io import StringIO

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db') 
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Update host if running inside Airflow container (Automatic Detection)
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

ORIGINAL_TABLE_NAME = "product_list_xlsx"
RAW_TABLE = f"raw_schema.{ORIGINAL_TABLE_NAME}"
STAGING_TABLE = f"staging1_schema.product_list_cleaned"

# =========================================================
#               TRANSFORMATION FUNCTIONS
# =========================================================
def remove_unnamed(df):
    if df.columns[0].lower().startswith("unnamed"):
        df = df.drop(df.columns[0], axis=1)
    return df

def fix_product_type_format(s):
    s = str(s).replace("_", " ")
    return s.strip().lower() or "unknown"

def replace_null_unknown(s):
    if pd.isna(s):
        return "unknown"
    s = str(s).strip()
    if s == "" or s.lower() in ["nan", "none", "null", "n/a", "<na>"]:
        return "unknown"
    return s

# =========================================================
#                   MAIN SCRIPT
# =========================================================
def main():
    print(f"Connecting to database at: {DB_HOST}...")
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as conn:
            pass
        print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {e}")
        exit(1)

    metadata = MetaData()
    print(f"Reading from raw table: {RAW_TABLE}")
    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", engine)

    # -------------------- APPLY TRANSFORMATIONS --------------------
    df = remove_unnamed(df)
    df["product_type"] = df["product_type"].apply(fix_product_type_format)
    df["product_type"] = df["product_type"].apply(replace_null_unknown)

    # Ensure price is numeric
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")  # invalid → NaN
    else:
        df["price"] = None

    # -------------------- COPY TO STAGING USING COPY --------------------
    print(f"Loading into: {STAGING_TABLE}")

    # Convert dataframe to CSV in memory
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Drop and recreate table manually with proper types
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE")

    cursor.execute(f"""
        CREATE TABLE {STAGING_TABLE} (
            product_id   VARCHAR(12),
            product_name VARCHAR(100),
            product_type VARCHAR(50),
            price        DECIMAL(5,2)
        );
    """)

    # COPY into table
    cursor.copy_expert(f"COPY {STAGING_TABLE} FROM STDIN WITH CSV HEADER", buffer)

    conn.commit()
    cursor.close()
    conn.close()

    print("\n✅ Transformation + Fast COPY completed.")

if __name__ == "__main__":
    main()
