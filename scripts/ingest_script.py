import os
import re
import csv
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text
from sqlalchemy.dialects.postgresql import (
    VARCHAR, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP
)

# ----------------------------------------------------------
# CONFIGURATION: SECURITY & SAFETY
# ----------------------------------------------------------
DROP_COLUMNS = [
    "credit_card_number", 
    "cvv", 
    "password", 
    "social_security_number"
]

# ----------------------------------------------------------
# 1. SETUP DATABASE CONNECTION
# ----------------------------------------------------------
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db') 
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Update host if running inside Airflow container (Automatic Detection)
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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

# ----------------------------------------------------------
# 2. CREATE SCHEMAS (Fixed for all SQLAlchemy versions)
# ----------------------------------------------------------
def create_schemas():
    """
    Ensures the 3 main layers exist before we load data.
    Using engine.begin() handles transaction commits automatically.
    """
    schemas = ["raw_schema", "staging1_schema", "staging2_schema", "star_schema"]
    try:
        # engine.begin() starts a transaction and auto-commits on success
        with engine.begin() as conn:
            for schema in schemas:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                print(f"🏗️  Schema check: {schema} exists.")
    except Exception as e:
        print(f"Failed creating schemas: {e}")
        raise e # Critical error, must stop script if schemas fail

# ----------------------------------------------------------
# 3. HELPER FUNCTIONS
# ----------------------------------------------------------
def load_csv_with_fallbacks(file_path):
    """ Tries comma -> tab -> autodetect """
    try:
        df = pd.read_csv(file_path, delimiter=",")
        if df.shape[1] > 1: return df
    except: pass
    try:
        df = pd.read_csv(file_path, delimiter="\t")
        if df.shape[1] > 1: return df
    except: pass
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(2048)
            dialect = csv.Sniffer().sniff(sample)
        df = pd.read_csv(file_path, delimiter=dialect.delimiter)
        return df
    except: return None

def load_file_to_dataframe(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == ".csv": return load_csv_with_fallbacks(file_path)
        elif ext == ".parquet": return pd.read_parquet(file_path)
        elif ext in [".pkl", ".pickle"]: return pd.read_pickle(file_path)
        elif ext == ".html": return pd.read_html(file_path)[0]
        elif ext == ".json": return pd.read_json(file_path)
        elif ext == ".xlsx": return pd.read_excel(file_path)
        else: return None
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def extract_table_name(filename):
    """
    Creates a unique table name per file: 'file.csv' -> 'file_csv'
    """
    base, ext = os.path.splitext(filename)
    base_clean = base.replace(" ", "_").replace("-", "_").lower()
    ext_clean = ext.replace(".", "").lower()
    return f"{base_clean}_{ext_clean}"

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype): return INTEGER
    elif pd.api.types.is_float_dtype(dtype): return FLOAT
    elif pd.api.types.is_bool_dtype(dtype): return BOOLEAN
    else: return VARCHAR

# ----------------------------------------------------------
# 4. LOGIC: Create Table in 'raw_schema'
# ----------------------------------------------------------
def create_table_if_not_exists(df, table_name):
    inspector = inspect(engine)
    if inspector.has_table(table_name, schema="raw_schema"):
        print(f"   -> Table 'raw_schema.{table_name}' already exists. Skipping creation.")
        return

    print(f"   -> Creating 'raw_schema.{table_name}'...")
    columns = []
    for col_name, dtype in df.dtypes.items():
        clean_col = col_name.replace(" ", "_").lower()
        columns.append(Column(clean_col, map_dtype(dtype)))

    table = Table(table_name, metadata, *columns, schema="raw_schema")
    table.create(engine)

# ----------------------------------------------------------
# 5. LOGIC: Insert Data into 'raw_schema'
# ----------------------------------------------------------
def load_dataframe_to_postgres(df, table_name):
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists="replace", # <--- Wipes old table and inserts fresh
            index=False,
            method='multi',
            chunksize=1000,
            schema="raw_schema"
        )
        print(f"   -> Inserted {len(df)} rows.")
    except Exception as e:
        print(f"   -> ❌ FAILED to insert {table_name}: {e}")
        # Note: I removed 'raise e' so the script keeps running for other files!

# ----------------------------------------------------------
# 6. FILE PROCESSOR
# ----------------------------------------------------------
def ingest_file(file_path):
    filename = os.path.basename(file_path)
    table_name = extract_table_name(filename)

    print(f"\n📄 Processing: {filename}")
    df = load_file_to_dataframe(file_path)

    if df is not None and not df.empty:
        cols_to_drop = [col for col in DROP_COLUMNS if col in df.columns]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)
            print(f"   🛡️ Dropped sensitive columns: {cols_to_drop}")

        create_table_if_not_exists(df, table_name)
        load_dataframe_to_postgres(df, table_name)
    else:
        print("   -> Skipped (Empty or invalid)")

# ----------------------------------------------------------
# 7. MAIN EXECUTION
# ----------------------------------------------------------
if __name__ == "__main__":
    # 1. Create the architecture
    create_schemas()

    # 2. Detect the correct Base Directory
    # This logic fixes the "Folder not found" errors
    if os.path.exists("/opt/airflow/Project Dataset"):
        BASE_DIR = "/opt/airflow/Project Dataset"
        print("🌍 Environment detected: Airflow Container")
    elif os.path.exists("/app/Project Dataset"):
        BASE_DIR = "/app/Project Dataset"
        print("🌍 Environment detected: ETL App Container")
    else:
        print("❌ CRITICAL ERROR: Could not find 'Project Dataset' folder.")
        print("Checked /opt/airflow/Project Dataset and /app/Project Dataset.")
        exit(1)

    departments = [
        "Business Department", "Customer Management Department", 
        "Enterprise Department", "Marketing Department", "Operations Department"
    ]

    files_found = 0

    for dept in departments:
        full_path = os.path.join(BASE_DIR, dept)
        if os.path.exists(full_path):
            print(f"\n🔍 Scanning: {dept}")
            files = os.listdir(full_path)
            for f in files:
                ingest_file(os.path.join(full_path, f))
                files_found += 1
        else:
            print(f"⚠️ Folder not found: {dept}")

    if files_found == 0:
        print("\n❌ NO FILES WERE INGESTED. Please check your folder structure.")
        exit(1)
    
    print("\n✅ All tasks finished successfully.")