import os
import io
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

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
    "user_type": "VARCHAR(8)" 
}

# =========================================================
#                GEOCODING SETUP (STATE ONLY)
# =========================================================
geolocator = Nominatim(user_agent="shopzada_user_data_elt")

def geocode_unique_cities(unique_cities):
    """
    Geocode unique cities to get STATE only.
    """
    geocode_map = {}
    total = len(unique_cities)

    print(f"\n🌍 Geocoding {total} unique cities (STATE only)...")

    for idx, city in enumerate(unique_cities, 1):
        if pd.isna(city) or str(city).strip() == "":
            geocode_map[city] = None
            continue

        city_clean = str(city).strip()

        if idx % 10 == 0 or idx == total:
            print(f"  📍 Progress: {idx}/{total}")

        for attempt in range(3):
            try:
                location = geolocator.geocode(
                    f"{city_clean}, United States",
                    exactly_one=True,
                    timeout=10,
                    addressdetails=True
                )

                if location and location.raw.get("address"):
                    geocode_map[city] = location.raw["address"].get("state")
                else:
                    geocode_map[city] = None

                time.sleep(0.5)
                break

            except (GeocoderTimedOut, GeocoderServiceError):
                time.sleep(1)
                if attempt == 2:
                    geocode_map[city] = None

    print("✅ Geocoding complete\n")
    return geocode_map

# =========================================================
#                DATA CLEANING
# =========================================================
def remove_unnamed(df):
    if df.columns[0].lower().startswith("unnamed"):
        return df.drop(df.columns[0], axis=1)
    return df


def clean_columns(df):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()

    # ---- 1. creation_date → DATE
    df["creation_date"] = (
        pd.to_datetime(df["creation_date"], errors="coerce")
        .dt.date
    )

    # ---- 3. City normalization
    df["city"] = df["city"].astype(str).str.strip().str.title()

    # ---- 4. STATE via Geopy (unique cities only)
    unique_cities = df["city"].dropna().unique()
    state_map = geocode_unique_cities(unique_cities)
    df["state"] = df["city"].map(state_map)

    # ---- 5. HARD ENFORCE COUNTRY
    df["country"] = "United States"

    return df.reset_index(drop=True)

# =========================================================
#                COPY LOAD
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
#                MAIN PIPELINE
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

    print("\n" + "=" * 60)
    print("📋 STAFF DATA PREVIEW")
    print("=" * 60)
    print(final_df.head(10))
    print(f"\n📊 Rows: {len(final_df)}")
    print(f"📊 States found: {final_df['state'].notna().sum()}")
    print("=" * 60 + "\n")

    write_to_staging(final_df)

    print("🎉 Staff data ELT pipeline completed successfully!\n")


if __name__ == "__main__":
    main()