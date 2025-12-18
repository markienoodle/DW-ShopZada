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
    "raw_schema.user_job_csv"
]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "user_job_cleaned"

DROP_COLUMNS = [0]

REQUIRED_COLUMNS = [
    "user_id",
    "name",
    "job_title",
    "job_level"
]

# =========================================================
#          DATA TYPE MAPPING (EXPLICIT SQL TYPES)
# =========================================================
DTYPE_MAPPING = {
    "user_id": "VARCHAR(9)",
    "name": "VARCHAR(40)",
    "job_title": "VARCHAR(20)",
    "job_level": "VARCHAR(20)"
}

# =========================================================
#                   DATA CLEANING
# =========================================================
def clean_and_capitalize(text):
    """
    Normalize text fields:
    - Strip whitespace
    - Title Case
    - Replace null/empty with 'Unknown'
    """
    if pd.isna(text) or str(text).strip() == "":
        return "Unknown"
    return str(text).strip().title()


def remove_unnamed(df):
    """
    Remove auto-generated unnamed index column
    (common from CSV imports)
    """
    if df.columns.size > 0 and df.columns[0].lower().startswith("unnamed"):
        df = df.drop(df.columns[0], axis=1)
    return df


def clean_columns(df):
    """
    Main data cleaning function for user_job pipeline
    """
    # Always work on a copy (Airflow-safe)
    df = df.copy()

    print(f"📋 Columns found: {list(df.columns)}")

    # Validate schema
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    # Columns requiring capitalization cleanup
    CAP_COLUMNS = ["name", "job_title", "job_level"]

    # Apply normalization
    for col in CAP_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_and_capitalize)

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
