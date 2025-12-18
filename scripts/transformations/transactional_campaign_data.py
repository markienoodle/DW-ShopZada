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
    "raw_schema.transactional_campaign_data_csv"
]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "transactional_campaign_data_cleaned"

REQUIRED_COLUMNS = [
    "transaction_date",
    "campaign_id",
    "order_id",
    "estimated_arrival",
    "availed"
]

# =========================================================
#          DATA TYPE MAPPING (EXPLICIT SQL TYPES)
# =========================================================
DTYPE_MAPPING = {
    "transaction_date": "DATE",
    "campaign_id": "VARCHAR(13)",
    "order_id": "VARCHAR(36)",
    "estimated_arrival": "INTEGER",
    "availed": "BOOLEAN"
}

# =========================================================
#                   DATA CLEANING
# =========================================================
def remove_unnamed(df):
    if df.columns[0].lower().startswith("unnamed"):
        df = df.drop(df.columns[0], axis=1)
    return df


def clean_columns(df):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    df["transaction_date"] = df["transaction_date"].astype(str)
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["order_id"] = df["order_id"].astype(str)
    df["estimated_arrival"] = df["estimated_arrival"].astype(str)
    df["availed"] = df["availed"].astype(str)

    # remove any non-numeric characters and convert to integer
    df["estimated_arrival"] = (
        df["estimated_arrival"]
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64")
        )
    
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

    # Create table if it does not exist
    col_defs = [f"{col} {dtype}" for col, dtype in DTYPE_MAPPING.items()]
    create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{STAGING_TABLE} (
            {', '.join(col_defs)}
        );
    """
    cursor.execute(create_stmt)

    # Clear existing rows each run
    cursor.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE};")


    # Convert DataFrame → CSV buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # COPY into staging table
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

    print(" Transactional Campaign data ELT pipeline completed.")


if __name__ == "__main__":
    main()
