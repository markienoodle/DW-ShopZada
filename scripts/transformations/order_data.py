import os
import io
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

# Detect Airflow container
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
SOURCE_TABLES = [
    "raw_schema.order_data_20200101_20200701_parquet",
    "raw_schema.order_data_20200701_20211001_pickle",
    "raw_schema.order_data_20211001_20220101_csv",
    "raw_schema.order_data_20220101_20221201_xlsx",
    "raw_schema.order_data_20221201_20230601_json",
    "raw_schema.order_data_20230601_20240101_html"
]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "order_data_cleaned"

DROP_COLUMNS = [0]

REQUIRED_COLUMNS = [
    "order_id",
    "user_id",
    "estimated_arrival",
    "transaction_date"
]

# =========================================================
#          DATA TYPE MAPPING (EXPLICIT SQL TYPES)
# =========================================================
DTYPE_MAPPING = {
    "order_id": "VARCHAR(36)",        # UUID-like
    "user_id": "VARCHAR(9)",         # alphanumeric
    "estimated_arrival": "INTEGER",   # numeric
    "transaction_date": "DATE"        # date only
}

# =========================================================
#                   DATA CLEANING
# =========================================================
def remove_unnamed(df):
    if df.columns[0].lower().startswith("unnamed"):
        df = df.drop(df.columns[0], axis=1)
    return df

def clean_columns(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # String normalization
    df["order_id"] = df["order_id"].astype(str)
    df["user_id"] = df["user_id"].astype(str)

    # Convert "9days" → 9
    df["estimated_arrival"] = (
        df["estimated_arrival"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64")
    )

    # Convert to DATE
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce"
    ).dt.date

    # Warnings
    if df["estimated_arrival"].isna().any():
        print("Warning: Some estimated_arrival values had no digits.")
    if df["transaction_date"].isna().any():
        print("Warning: Some transaction_date values could not be parsed.")

    return df

# =========================================================
#                COPY LOADING FUNCTION
# =========================================================
def write_to_staging(df):
    engine = create_engine(DB_URI)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Ensure schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")

    # Reset table
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{STAGING_TABLE};")

    # Build CREATE TABLE DDL using explicit VARCHAR/INTEGER/DATE
    col_defs = [f"{col} {dtype}" for col, dtype in DTYPE_MAPPING.items()]
    create_stmt = f"""
        CREATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE} (
            {', '.join(col_defs)}
        );
    """
    cursor.execute(create_stmt)

    # Convert DF → CSV buffer for COPY
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # COPY into Postgres
    cursor.copy_expert(
        f"""
        COPY {STAGING_SCHEMA}.{STAGING_TABLE}
        FROM STDIN
        WITH CSV NULL '' DELIMITER ',';
        """,
        buffer
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows into {STAGING_SCHEMA}.{STAGING_TABLE} via COPY.")

# =========================================================
#                   MAIN PIPELINE
# =========================================================
def main():
    engine = create_engine(DB_URI)

    frames = []

    for table in SOURCE_TABLES:
        print(f"Loading: {table}")
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        df = clean_columns(remove_unnamed(df))
        frames.append(df)

    final_df = pd.concat(frames, ignore_index=True)

    write_to_staging(final_df)

    print("Order data ELT pipeline completed.")


if __name__ == "__main__":
    main()
