import os
import re
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from sqlalchemy.dialects.postgresql import (
    VARCHAR, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP
)

# ----------------------------------------------------------
# CONFIGURATION: SECURITY & SAFETY
# ----------------------------------------------------------
# Columns to ALWAYS drop for privacy/security
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

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"🔌 Connecting to database at: {DB_HOST}...")

try:
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()
    print("✅ Connection successful!")
    connection.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

metadata = MetaData()

# ----------------------------------------------------------
# 2. HELPER: Load a single file → DataFrame
# ----------------------------------------------------------
def load_file_to_dataframe(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    
    try:
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
            print(f"⚠️ Unsupported file type: {ext}")
            return None
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return None

# ----------------------------------------------------------
# 3. HELPER: Extract table name from filename
# ----------------------------------------------------------
def extract_table_name(filename):
    base = os.path.splitext(filename)[0]
    match = re.split(r"\d", base, maxsplit=1)[0]
    clean_name = match.rstrip("_").lower()
    return clean_name

# ----------------------------------------------------------
# 4. HELPER: Map pandas dtype → PostgreSQL type
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
# 5. LOGIC: Create table if missing
# ----------------------------------------------------------
def create_table_if_not_exists(df, table_name):
    inspector = inspect(engine)
    
    if inspector.has_table(table_name):
        print(f"   -> Table '{table_name}' already exists. Appending data...")
        return

    print(f"   -> Table '{table_name}' does not exist. Creating it...")
    
    columns = []
    for col_name, dtype in df.dtypes.items():
        clean_col = col_name.replace(" ", "_").lower()
        pg_type = map_dtype(dtype)
        columns.append(Column(clean_col, pg_type))
    
    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)
    print(f"   -> Table '{table_name}' created successfully.")

# ----------------------------------------------------------
# 6. LOGIC: Insert dataframe into PostgreSQL
# ----------------------------------------------------------
def load_dataframe_to_postgres(df, table_name):
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]
    
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists="append", 
            index=False,
            method='multi',      
            chunksize=1000       
        )
        print(f"   -> ✅ Inserted {len(df)} rows into '{table_name}'.")
    except Exception as e:
        print(f"   -> ❌ Failed to insert data: {e}")

# ----------------------------------------------------------
# 7. LOGIC: Ingest a single file
# ----------------------------------------------------------
def ingest_file(file_path):
    filename = os.path.basename(file_path)
    table_name = extract_table_name(filename)

    print(f"\n📄 Processing: {filename}")
    
    df = load_file_to_dataframe(file_path)
    
    if df is not None and not df.empty:
        # --- SECURITY CHECK ---
        # Check if any columns match our blacklist and drop them
        initial_cols = len(df.columns)
        
        # Drop columns if they exist in the dataframe
        cols_to_drop = [col for col in DROP_COLUMNS if col in df.columns]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)
            print(f"   🛡️ SECURITY: Dropped sensitive columns: {cols_to_drop}")
        
        create_table_if_not_exists(df, table_name)
        load_dataframe_to_postgres(df, table_name)
    else:
        print("   -> Skipped (Empty or invalid dataframe)")

# ----------------------------------------------------------
# 8. LOGIC: Scan Folder
# ----------------------------------------------------------
def ingest_folder(base_folder_path):
    print(f"\n🔍 Scanning folder: {base_folder_path}")
    
    if not os.path.exists(base_folder_path):
        print(f"❌ Folder not found: {base_folder_path}")
        print("   -> Are you running this inside Docker? Check your volume mounts.")
        return

    supported_exts = [".csv", ".parquet", ".pkl", ".pickle", ".html", ".json", ".xlsx"]
    
    files = [
        os.path.join(base_folder_path, f)
        for f in os.listdir(base_folder_path)
        if os.path.splitext(f)[1].lower() in supported_exts
    ]

    if not files:
        print("   -> No supported files found.")
        return

    print(f"   -> Found {len(files)} files.")

    for file_path in files:
        ingest_file(file_path)

# ----------------------------------------------------------
# 9. MAIN EXECUTION
# ----------------------------------------------------------
if __name__ == "__main__":
    
    BASE_DIR = "/app/Project Dataset"

    departments = [
        "Business Department",
        "Customer Management Department",
        "Enterprise Department",
        "Marketing Department",
        "Operations Department"
    ]

    for dept in departments:
        full_path = os.path.join(BASE_DIR, dept)
        ingest_folder(full_path)

    print("\n🎉 =========================================")
    print("🎉 All ingestion tasks finished.")
    print("🎉 =========================================")