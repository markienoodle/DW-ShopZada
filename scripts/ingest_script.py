import os
import re
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
# 2. CREATE SCHEMAS (Medallion Architecture)
# ----------------------------------------------------------
def create_schemas():
    """
    Ensures the 3 main layers exist before we load data.
    """
    schemas = ["raw_schema", "staging1_schema", "staging2_schema", "star_schema"]
    
    try:
        with engine.connect() as conn:
            for schema in schemas:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                print(f"🏗️  Schema check: {schema} exists.")
            conn.commit()
    except Exception as e:
        print(f"❌ Failed creating schemas: {e}")

# ----------------------------------------------------------
# 3. HELPER FUNCTIONS
# ----------------------------------------------------------
def load_file_to_dataframe(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == ".csv": return pd.read_csv(file_path)
        elif ext == ".parquet": return pd.read_parquet(file_path)
        elif ext in [".pkl", ".pickle"]: return pd.read_pickle(file_path)
        elif ext == ".html": return pd.read_html(file_path)[0]
        elif ext == ".json": return pd.read_json(file_path)
        elif ext == ".xlsx": return pd.read_excel(file_path)
        else: return None
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return None

def extract_table_name(filename):
    base = os.path.splitext(filename)[0]
    match = re.split(r"\d", base, maxsplit=1)[0]
    return match.rstrip("_").lower()

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype): return INTEGER
    elif pd.api.types.is_float_dtype(dtype): return FLOAT
    elif pd.api.types.is_bool_dtype(dtype): return BOOLEAN
    elif pd.api.types.is_datetime64_any_dtype(dtype): return TIMESTAMP
    else: return VARCHAR

# ----------------------------------------------------------
# 4. LOGIC: Create Table in 'raw_schema'
# ----------------------------------------------------------
def create_table_if_not_exists(df, table_name):
    inspector = inspect(engine)
    
    # Check specifically in raw_schema
    if inspector.has_table(table_name, schema="raw_schema"):
        print(f"   -> Table 'raw_schema.{table_name}' exists. Appending...")
        return

    print(f"   -> Creating 'raw_schema.{table_name}'...")
    columns = []
    for col_name, dtype in df.dtypes.items():
        clean_col = col_name.replace(" ", "_").lower()
        pg_type = map_dtype(dtype)
        columns.append(Column(clean_col, pg_type))
    
    # Force creation in raw_schema
    table = Table(table_name, metadata, *columns, schema="raw_schema")
    metadata.create_all(engine)

# ----------------------------------------------------------
# 5. LOGIC: Insert Data into 'raw_schema'
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
            chunksize=1000,
            schema="raw_schema"  # <--- CRITICAL: Puts data in the correct folder
        )
        print(f"   -> ✅ Inserted {len(df)} rows.")
    except Exception as e:
        print(f"   -> ❌ Failed to insert: {e}")

# ----------------------------------------------------------
# 6. FILE PROCESSOR
# ----------------------------------------------------------
def ingest_file(file_path):
    filename = os.path.basename(file_path)
    table_name = extract_table_name(filename)

    print(f"\n📄 Processing: {filename}")
    df = load_file_to_dataframe(file_path)
    
    if df is not None and not df.empty:
        # Security Filter
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
    # 1. Create the architecture first
    create_schemas()

    # 2. Ingest the files
    BASE_DIR = "/app/Project Dataset"
    departments = [
        "Business Department", "Customer Management Department", 
        "Enterprise Department", "Marketing Department", "Operations Department"
    ]

    for dept in departments:
        full_path = os.path.join(BASE_DIR, dept)
        if os.path.exists(full_path):
            print(f"\n🔍 Scanning: {dept}")
            files = os.listdir(full_path)
            for f in files:
                ingest_file(os.path.join(full_path, f))
        else:
            print(f"⚠️ Folder not found: {dept}")

    print("\n🎉 All tasks finished.")
