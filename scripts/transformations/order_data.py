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

REQUIRED_COLUMNS = [
    "order_id",
    "campaign_id",
    "user_id",
    "merchant_id",
    "staff_id",
    "transaction_date",
    "estimated_arrival",
    "delay_in_days",
    "availed"
]

DTYPE_MAPPING = {
    "order_id": "VARCHAR(36)",
    "campaign_id": "VARCHAR(13)",
    "user_id": "VARCHAR(9)",
    "merchant_id": "VARCHAR(13)",
    "staff_id": "VARCHAR(12)",
    "transaction_date": "DATE",
    "estimated_arrival": "INTEGER",
    "delay_in_days": "INTEGER",
    "availed": "BOOLEAN",
    "ingested_at": "TIMESTAMP"
}
# =========================================================
#                TERMINAL CHECKS
# =========================================================
def check_merge(df, table_name, merge_col):
    print(f"\n=== After merging table: {table_name} on '{merge_col}' ===")
    print(f"Total rows: {len(df)}")
    
# =========================================================
#                MERGE FUNCTIONS
# =========================================================
def merge_source_tables(merge_column="unnamed_0"):
    """Merge all SOURCE_TABLES together"""
    engine = create_engine(DB_URI)
    merged_df = None
    for table in SOURCE_TABLES:
        print(f"Loading {table}")
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(
                merged_df,
                df,
                how="outer",  # preserve all rows
                on=merge_column,
                suffixes=("", "_dup")
            )
            merged_df = merged_df.loc[:, ~merged_df.columns.str.endswith("_dup")]
    print(f"Completed merge of SOURCE_TABLES: {len(merged_df)} rows")
    return merged_df

def merge_additional_tables(merged_df, additional_tables):
    """Incrementally merge additional tables onto merged_df"""
    engine = create_engine(DB_URI)
    for table_name, join_col in additional_tables:
        print(f"\nMerging additional table: {table_name} on {join_col}")
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        merged_df = pd.merge(
            merged_df,
            df,
            how="left",
            on=join_col,
            suffixes=("", "_new")
        )
        merged_df = merged_df.loc[:, ~merged_df.columns.str.endswith("_new")]
        check_merge(merged_df, table_name, join_col)
    return merged_df

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

    df["order_id"] = df["order_id"].astype(str)
    df["user_id"] = df["user_id"].astype(str)

    df["estimated_arrival"] = (
        df["estimated_arrival"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64")
    )

    df["delay_in_days"] = (
        df["delay_in_days"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64")
    )

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce"
    ).dt.date

    return df

# =========================================================
#                WRITE TO STAGING
# =========================================================
def write_to_staging(df):
    engine = create_engine(DB_URI)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{STAGING_TABLE};")

    col_defs = [f"{col} {dtype}" for col, dtype in DTYPE_MAPPING.items()]
    create_stmt = f"""
        CREATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE} (
            {', '.join(col_defs)}
        );
    """
    cursor.execute(create_stmt)

    # Ensure column order
    df = df[[col for col in DTYPE_MAPPING.keys()]]

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

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

    # Step 1: Merge source tables
    merged_df = merge_source_tables(merge_column="unnamed_0")

    # Step 2: Merge additional tables
    additional_tables = [
        ("raw_schema.order_delays_html", "unnamed_0"),
        ("raw_schema.order_with_merchant_data1_parquet", "order_id"),
        ("raw_schema.order_with_merchant_data2_parquet", "order_id"),
        ("raw_schema.order_with_merchant_data3_csv", "order_id"),
        ("raw_schema.transactional_campaign_data_csv", "order_id")
    ]
    merged_df = merge_additional_tables(merged_df, additional_tables)

    # Step 3: Clean and write to staging
    final_df = clean_columns(remove_unnamed(merged_df))
    write_to_staging(final_df)

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
