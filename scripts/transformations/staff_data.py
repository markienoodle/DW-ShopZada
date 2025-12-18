import os
import io
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time

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
    "raw_schema.staff_data_html"
]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "staff_data_cleaned"

REQUIRED_COLUMNS = [
    "staff_id",
    "name",
    "job_level",
    "street",
    "state",
    "city",
    "country",
    "contact_number",
    "creation_date"
]

# =========================================================
#          DATA TYPE MAPPING (EXPLICIT SQL TYPES)
# =========================================================
DTYPE_MAPPING = {
    "staff_id": "VARCHAR(12)",
    "name": "VARCHAR(50)",
    "job_level": "VARCHAR(20)",
    "street": "VARCHAR(40)",
    "state": "VARCHAR(27)",
    "city": "VARCHAR(20)",
    "country": "VARCHAR(52)",
    "contact_number": "VARCHAR(20)",
    "creation_date": "DATE"
}

# =========================================================
#                GEOCODING SETUP
# =========================================================
geolocator = Nominatim(user_agent="shopzada_staff_elt")

def geocode_unique_cities(unique_cities):
    """
    Geocode only unique cities and return a mapping dictionary.
    This is MUCH faster than geocoding every row individually.
    """
    geocode_map = {}
    total = len(unique_cities)
    
    print(f"\n🌍 Geocoding {total} unique cities...")
    
    for idx, city in enumerate(unique_cities, 1):
        if pd.isna(city) or str(city).strip() == "":
            geocode_map[city] = {'state': None, 'country': 'United States'}
            continue
        
        city_clean = str(city).strip()
        
        # Progress indicator
        if idx % 10 == 0 or idx == total:
            print(f"  📍 Progress: {idx}/{total} unique cities processed...")
        
        # Try geocoding with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                location = geolocator.geocode(
                    f"{city_clean}, United States",
                    exactly_one=True,
                    timeout=10,
                    addressdetails=True
                )
                
                if location and location.raw.get('address'):
                    address = location.raw['address']
                    state = address.get('state', None)
                    geocode_map[city] = {'state': state, 'country': 'United States'}
                else:
                    geocode_map[city] = {'state': None, 'country': 'United States'}
                
                # Rate limiting - be nice to the API
                time.sleep(0.5)
                break
                
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    print(f"  ⚠️  Geocoding failed for '{city}': {e}")
                    geocode_map[city] = {'state': None, 'country': 'United States'}
    
    print(f"✅ Geocoding complete! Mapped {len(geocode_map)} unique cities.\n")
    return geocode_map

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

    # ---- 1. Clean creation_date → date only
    df["creation_date"] = (
        pd.to_datetime(df["creation_date"], errors="coerce")
        .dt.date
    )

    # ---- 2. Clean contact_number → digits only
    df["contact_number"] = (
        df["contact_number"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)  
        .replace("", np.nan)      
    )

    # ---- 3. OPTIMIZED GEOCODING: Only geocode unique cities
    print(f"\n📊 Dataset info: {len(df)} total rows")
    
    # Get unique cities
    unique_cities = df['city'].dropna().unique()
    print(f"📊 Found {len(unique_cities)} unique cities to geocode")
    
    # Geocode only unique cities
    geocode_map = geocode_unique_cities(unique_cities)
    
    # Map results back to all rows (FAST operation)
    print("🔄 Mapping geocoded results to all rows...")
    df['state'] = df['city'].map(lambda x: geocode_map.get(x, {}).get('state'))
    df['country'] = df['city'].map(lambda x: geocode_map.get(x, {}).get('country', 'United States'))
    
    print("✅ Mapping complete!")

    return df.reset_index(drop=True)

# =========================================================
#                COPY LOADING FUNCTION
# =========================================================
def write_to_staging(df):
    engine = create_engine(DB_URI)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Ensure schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")

    # Reset table (order-style)
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{STAGING_TABLE};")

    col_defs = [f"{col} {dtype}" for col, dtype in DTYPE_MAPPING.items()]
    create_stmt = f"""
        CREATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE} (
            {', '.join(col_defs)}
        );
    """
    cursor.execute(create_stmt)

    # Convert DataFrame → CSV buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # COPY load
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

    print(f"✅ Loaded {len(df)} rows into {STAGING_SCHEMA}.{STAGING_TABLE} via COPY.")

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

    # ================= DISPLAY =================
    print("\n" + "="*50)
    print("📋 STAFF DATA PREVIEW")
    print("="*50)
    print(final_df.head(10))
    print(f"\n📊 Shape: {final_df.shape}")
    print(f"📊 Columns: {list(final_df.columns)}")
    
    # Show geocoding results
    print("\n" + "="*50)
    print("🗺️  GEOCODING RESULTS")
    print("="*50)
    print(f"Total records: {len(final_df)}")
    print(f"Records with state found: {final_df['state'].notna().sum()}")
    print(f"Records with no state: {final_df['state'].isna().sum()}")
    print("\n📊 Top 10 States:")
    print(final_df['state'].value_counts().head(10))
    print("="*50 + "\n")
    # ===========================================

    write_to_staging(final_df)

    print("🎉 Staff data ELT pipeline completed successfully!\n")


if __name__ == "__main__":
    main()