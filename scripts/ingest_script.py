import os
import io
import re
import csv
import pandas as pd
from datetime import datetime  # <-- add
from sqlalchemy import create_engine, MetaData, text

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

if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Connecting to database at: {DB_HOST}...")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        pass
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

metadata = MetaData()

# ----------------------------------------------------------
# 2. CREATE SCHEMAS
# ----------------------------------------------------------
def create_schemas():
    schemas = ["control_schema", "raw_schema", "staging1_schema", "staging2_schema", "star_schema"]
    try:
        with engine.begin() as conn:
            for schema in schemas:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                print(f"🏗️  Schema check: {schema} exists.")
    except Exception as e:
        print(f"Failed creating schemas: {e}")
        raise e

# ----------------------------------------------------------
# 2b. CONTROL TABLE FOR INGEST TRACKING
# ----------------------------------------------------------
def create_control_table():
    """
    Creates a single control table that tracks latest ingest timestamp per table.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS control_schema.table_ingest_audit (
            schema_name      TEXT NOT NULL,
            table_name       TEXT NOT NULL,
            last_ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (schema_name, table_name)
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("📋 Control table: control_schema.table_ingest_audit is ready.")

def upsert_ingest_audit(schema_name: str, table_name: str, ingested_at: datetime):
    """
    Upsert latest ingest timestamp for a given table.
    """
    sql = """
        INSERT INTO control_schema.table_ingest_audit (schema_name, table_name, last_ingested_at)
        VALUES (:schema_name, :table_name, :ingested_at)
        ON CONFLICT (schema_name, table_name)
        DO UPDATE SET last_ingested_at = GREATEST(
            EXCLUDED.last_ingested_at,
            control_schema.table_ingest_audit.last_ingested_at
        );
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "ingested_at": ingested_at,
            }
        )

# ----------------------------------------------------------
# 3. HELPER FUNCTIONS
# ----------------------------------------------------------
def load_csv_with_fallbacks(file_path):
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
    except: 
        return None

def load_file_to_dataframe(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == ".csv":
            df = load_csv_with_fallbacks(file_path)
        elif ext == ".parquet":
            df = pd.read_parquet(file_path)
        elif ext in [".pkl", ".pickle"]:
            df = pd.read_pickle(file_path)
        elif ext == ".html":
            df = pd.read_html(file_path)[0]
        elif ext == ".json":
            df = pd.read_json(file_path)
        elif ext == ".xlsx":
            df = pd.read_excel(file_path)
        else:
            return None

        if df is not None:
            if not isinstance(df.index, pd.RangeIndex):
                df.reset_index(inplace=True)
                if 'index' in df.columns:
                    df.rename(columns={'index': 'unnamed_0'}, inplace=True)

            unnamed_cols = [c for c in df.columns if re.match(r'^Unnamed: 0$', c, re.IGNORECASE)]
            for col in unnamed_cols:
                df.rename(columns={col: 'unnamed_0'}, inplace=True)

        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def extract_table_name(filename):
    base, ext = os.path.splitext(filename)
    base_clean = base.replace(" ", "_").replace("-", "_").lower()
    ext_clean = ext.replace(".", "").lower()
    return f"{base_clean}_{ext_clean}"

# ----------------------------------------------------------
# 4. LOGIC: Create Tables and Insert Data into 'raw_schema'
# ----------------------------------------------------------
def load_dataframe_to_postgres(df, table_name):
    # --- SANITIZE COLUMN NAMES ---
    def sanitize(col):
        col = col.lower()
        col = re.sub(r"[^\w]", "_", col)
        col = re.sub(r"_+", "_", col)
        col = col.strip("_")
        return col

    df.columns = [sanitize(c) for c in df.columns]

    # --- ADD ingested_at COLUMN (in-memory) ---
    current_ingest_ts = datetime.utcnow()
    df["ingested_at"] = current_ingest_ts.isoformat()

    csv_data = df.to_csv(index=False)

    conn = engine.raw_connection()
    cur = conn.cursor()

    try:
        # 1. Ensure table has all data columns as TEXT plus ingested_at as TIMESTAMPTZ
        data_columns = [c for c in df.columns if c != "ingested_at"]
        columns_sql = ", ".join([f"{col} TEXT" for col in data_columns] + ["ingested_at TIMESTAMPTZ"])
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS raw_schema.{table_name} (
                {columns_sql}
            );
        """
        cur.execute(create_sql)

        # 1b. Make sure ingested_at column exists if table is older
        alter_sql = f"""
            ALTER TABLE raw_schema.{table_name}
            ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;
        """
        cur.execute(alter_sql)

        # 2. Append using COPY (all columns including ingested_at)
        copy_sql = f"""
            COPY raw_schema.{table_name} ({', '.join(df.columns)})
            FROM STDIN WITH CSV HEADER;
        """
        cur.copy_expert(copy_sql, io.StringIO(csv_data))

        conn.commit()
        print(f"✅ Appended {len(df)} rows into raw_schema.{table_name}")

    except Exception as e:
        conn.rollback()
        print(f"❌ COPY failed for {table_name}: {e}")

    finally:
        cur.close()
        conn.close()

    # 3. Update control table with latest ingest time
    upsert_ingest_audit("raw_schema", table_name, current_ingest_ts)

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

        load_dataframe_to_postgres(df, table_name)
    else:
        print("   -> Skipped (Empty or invalid)")

# ----------------------------------------------------------
# 7. MAIN EXECUTION
# ----------------------------------------------------------
if __name__ == "__main__":
    # 1. Create the architecture
    create_schemas()
    create_control_table()

    # 2. Detect the correct Base Directory
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
