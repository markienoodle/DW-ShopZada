import os
import re
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.dialects.postgresql import (
    VARCHAR, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP
)

# ----------------------------------------------------------
# 1. EDIT THIS BLOCK FOR YOUR LOCAL POSTGRES INSTANCE
# ----------------------------------------------------------
DATABASE_URL = "postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE"
engine = create_engine(DATABASE_URL)
metadata = MetaData()


# ----------------------------------------------------------
# 2. Load a single file → DataFrame
# ----------------------------------------------------------
def load_file_to_dataframe(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext == ".parquet":
        return pd.read_parquet(file_path)
    elif ext in [".pkl", ".pickle"]:
        return pd.read_pickle(file_path)
    elif ext == ".html":
        return pd.read_html(file_path)[0]
    elif ext == ".json":
        return pd.read_json(file_path)
    elif ext == ".xlsx":
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


# ----------------------------------------------------------
# 3. Extract table name from filename
# ----------------------------------------------------------
def extract_table_name(filename):
    """
    Extracts table name by taking everything before the first digit.
    Example:
        line_item_data_prices1.csv → line_item_data_prices
        order_data_20200101-20200701.parquet → order_data_
    """

    base = os.path.splitext(filename)[0]

    # Split before first digit
    match = re.split(r"\d", base, maxsplit=1)[0]

    # Remove trailing underscore, if present
    match = match.rstrip("_")

    return match.lower()


# ----------------------------------------------------------
# 4. Map pandas dtype → PostgreSQL type
# ----------------------------------------------------------
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return INTEGER
    elif pd.api.types.is_float_dtype(dtype):
        return FLOAT
    elif pd.api.types.is_bool_dtype(dtype):
        return BOOLEAN
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return TIMESTAMP
    else:
        return VARCHAR


# ----------------------------------------------------------
# 5. Create table if missing
# ----------------------------------------------------------
def create_table_if_not_exists(df, table_name):
    metadata.reflect(bind=engine)

    if table_name in metadata.tables:
        print(f"Table '{table_name}' already exists. Skipping creation.")
        return

    print(f"Creating table '{table_name}'...")

    columns = []
    for col_name, dtype in df.dtypes.items():
        pg_type = map_dtype(dtype)
        columns.append(Column(col_name, pg_type))

    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)

    print(f"Table '{table_name}' created.")


# ----------------------------------------------------------
# 6. Insert dataframe into PostgreSQL
# ----------------------------------------------------------
def load_dataframe_to_postgres(df, table_name):
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )
    print(f"Inserted {len(df)} rows into '{table_name}'.")


# ----------------------------------------------------------
# 7. Ingest a single file (with automatic table grouping)
# ----------------------------------------------------------
def ingest_file(file_path):
    filename = os.path.basename(file_path)
    table_name = extract_table_name(filename)

    print(f"\n📄 File: {filename}")
    print(f"➡ Target table: {table_name}")

    df = load_file_to_dataframe(file_path)

    create_table_if_not_exists(df, table_name)
    load_dataframe_to_postgres(df, table_name)

    print(f"✅ Completed ingestion for: {filename}")


# ----------------------------------------------------------
# 8. INGEST ALL FILES IN A FOLDER
# ----------------------------------------------------------
def ingest_folder(folder_path):
    print(f"\n🔍 Scanning folder: {folder_path}")

    supported_exts = [".csv", ".parquet", ".pkl", ".pickle", ".html", ".json", ".xlsx"]
    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.splitext(f)[1].lower() in supported_exts
    ]

    if not files:
        print("❌ No supported files found.")
        return

    print(f"📁 Found {len(files)} files.\n")

    for file_path in files:
        ingest_file(file_path)

    print("\n🎉 All files ingested successfully!")


# ----------------------------------------------------------
# 9. Example usage
# ----------------------------------------------------------
if __name__ == "__main__":
    folder_path = "data_files"  # ← change this to your folder path
    ingest_folder(folder_path)

# ASSUMES ALL FILES ARE IN ONE FOLDER
# EDIT THE VARIABLES DATABASE_URL AND folder_path