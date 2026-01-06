import os
import io
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Airflow container override
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
SOURCE_TABLES = ["raw_schema.user_data_json"]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "user_data_cleaned"

REQUIRED_COLUMNS = [
    "user_id",
    "creation_date",
    "name",
    "street",
    "state",
    "city",
    "country",
    "birthdate",
    "gender",
    "device_address",
    "user_type"
]

DTYPE_MAPPING = {
    "user_id": "VARCHAR(9)",
    "creation_date": "DATE",
    "name": "VARCHAR(40)",
    "street": "VARCHAR(40)",
    "state": "VARCHAR(27)",
    "city": "VARCHAR(20)",
    "country": "VARCHAR(52)",
    "birthdate": "TIMESTAMP",
    "gender": "VARCHAR(6)",
    "device_address": "VARCHAR(17)",
    "user_type": "VARCHAR(8)",
    "ingested_at": "TIMESTAMP"
}

# =========================================================
#                NETWORK SESSION WITH FALLBACKS
# =========================================================
def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = create_session()

# =========================================================
#            DETERMINISTIC STATE FALLBACK MAP
# =========================================================
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island',
    'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia',
    'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin',
    'WY': 'Wyoming', 'DC': 'District of Columbia'
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

    df = df.copy()

    # ---- 1. creation_date → DATE
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce").dt.date

    # ---- 2. birthdate → TIMESTAMP
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    # ---- 3. State normalization fallback
    df["state"] = (
        df["state"]
        .astype(str)
        .str.strip()
        .replace("", np.nan)
        .map(lambda x: US_STATES.get(x.upper(), x) if isinstance(x, str) else x)
    )

    # ---- 4. Country fallback
    df["country"] = df["country"].fillna("United States")

    # ---- 5. Ingestion timestamp
    df["ingested_at"] = pd.Timestamp.utcnow()

    return df.reset_index(drop=True)

# =========================================================
#                COPY LOADING FUNCTION
# =========================================================
def write_to_staging(df):
    engine = create_engine(DB_URI)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{STAGING_TABLE};")

    col_defs = [f"{c} {t}" for c, t in DTYPE_MAPPING.items()]
    cursor.execute(f"""
        CREATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE} (
            {', '.join(col_defs)}
        );
    """)

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

    print(f"✅ Loaded {len(df)} rows into {STAGING_SCHEMA}.{STAGING_TABLE}")

# =========================================================
#                   MAIN PIPELINE
# =========================================================
def main():
    engine = create_engine(DB_URI)
    frames = []

    for table in SOURCE_TABLES:
        print(f"📥 Loading: {table}")
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        df = clean_columns(remove_unnamed(df))
        frames.append(df)

    final_df = pd.concat(frames, ignore_index=True)

    print("\n📊 USER DATA SUMMARY")
    print(f"Rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print(final_df.head(10))

    write_to_staging(final_df)
    print("🎉 User data ELT pipeline completed successfully!")

if __name__ == "__main__":
    main()
