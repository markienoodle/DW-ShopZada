import os
import io
import pandas as pd
from sqlalchemy import create_engine

# =========================================================
#                 DATABASE & TABLE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "line_item_data_cleaned"

REQUIRED_COLUMNS = ["order_id", "price", "quantity", "product_name", "product_id"]

DTYPE_MAPPING = {
    "order_id": "VARCHAR(36)",
    "price": "DECIMAL(10,2)",
    "quantity": "INTEGER",
    "product_name": "VARCHAR(100)",
    "product_id": "VARCHAR(12)",
    "ingested_at": "TIMESTAMP"
}

# =========================================================
#                PROCESSING & DB FUNCTIONS
# =========================================================

def setup_staging_table():
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
        conn.execute(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{STAGING_TABLE};")
        col_defs = [f"{col} {dtype}" for col, dtype in DTYPE_MAPPING.items()]
        create_stmt = f"CREATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE} ({', '.join(col_defs)});"
        conn.execute(create_stmt)
    print(f"Table {STAGING_TABLE} prepared.")

def append_to_staging(df):
    engine = create_engine(DB_URI)
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cursor:
            df = df[[col for col in DTYPE_MAPPING.keys()]]
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(
                f"COPY {STAGING_SCHEMA}.{STAGING_TABLE} FROM STDIN WITH CSV NULL '' DELIMITER ',';",
                buffer
            )
        conn.commit()
    finally:
        conn.close()

# =========================================================
#                    MAIN PIPELINE
# =========================================================

def process_with_broadcast_join(price_tables, product_tables, chunk_size=150000):
    engine = create_engine(DB_URI)
    setup_staging_table()

    # 1. Load the "Right" side (Products) fully into memory first
    # We identify the first column as 'unnamed_0'
    print("Loading product lookup tables into memory...")
    product_dfs = []
    for table in product_tables:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        product_dfs.append(df)
    
    # Stack all product tables into one lookup dataframe
    lookup_df = pd.concat(product_dfs, ignore_index=True)
    
    # Identify the 'unnamed' column name dynamically
    join_key = [c for c in lookup_df.columns if c.lower().startswith('unnamed')][0]
    print(f"Joining on key: {join_key}")

    # 2. Stream the "Left" side (Prices) in chunks
    for p_table in price_tables:
        print(f"--- Streaming Price Table: {p_table} ---")
        p_iter = pd.read_sql(f"SELECT * FROM {p_table}", engine, chunksize=chunk_size)

        for i, p_chunk in enumerate(p_iter):
            # Perform the actual merge on the unnamed index column
            combined = pd.merge(
                p_chunk, 
                lookup_df, 
                on=join_key, 
                how="left",
                suffixes=("", "_prod")
            )

            # Clean and Write
            final_df = clean_columns(remove_unnamed(combined))
            append_to_staging(final_df)
            print(f"   Processed chunk {i+1}")

def remove_unnamed(df):
    unnamed_cols = [col for col in df.columns if col.lower().startswith("unnamed")]
    return df.drop(columns=unnamed_cols)

def clean_columns(df):
    df["quantity"] = (
        df["quantity"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64")
    )
    return df

def main():
    price_tables = [
        "raw_schema.line_item_data_prices1_csv",
        "raw_schema.line_item_data_prices2_csv",
        "raw_schema.line_item_data_prices3_parquet"
    ]
    product_tables = [
        "raw_schema.line_item_data_products1_csv",
        "raw_schema.line_item_data_products2_csv",
        "raw_schema.line_item_data_products3_parquet",
    ]

    process_with_broadcast_join(price_tables, product_tables, chunk_size=150000)
    print("\nPipeline completed successfully.")

if __name__ == "__main__":
    main()