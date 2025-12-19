import os
import io
import pandas as pd
from typing import Optional
from sqlalchemy import create_engine

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "shopzada_dwh")

# Detect Airflow container
if os.path.exists("/opt/airflow"):
    DB_HOST = "shopzada-postgres-db"

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
SOURCE_TABLES = [
    "raw_schema.user_credit_card_pickle"
]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "user_credit_card_cleaned"

REQUIRED_COLUMNS = [
    "user_id",
    "name",
    "issuing_bank"
]

# =========================================================
#          DATA TYPE MAPPING (EXPLICIT SQL TYPES)
# =========================================================
DTYPE_MAPPING = {
    "user_id": "VARCHAR(9)",
    "name": "VARCHAR(40)",
    "issuing_bank": "VARCHAR(20)",
    "ingested_at": "TIMESTAMP"
}

# =========================================================
#                   DATA CLEANING
# =========================================================
class BankCleaner:
    """
    Standardizes issuing bank names.
    Keeps known acronyms fully uppercase, others title-cased.
    """

    ACRONYMS = {
        "BDO", "BPI", "PNB", "DBP", "RCBC", "UCPB",
        "CHINABANK", "METROBANK", "MAYABANK",
        "ROBINSONSBANK", "SECURITYBANK", "EASTWEST"
    }

    @classmethod
    def clean_bank_name(cls, value: str) -> Optional[str]:
        if not isinstance(value, str) or not value.strip():
            return None

        words = value.strip().split()
        cleaned_words = []

        for word in words:
            if word.upper() in cls.ACRONYMS:
                cleaned_words.append(word.upper())
            else:
                cleaned_words.append(word.capitalize())

        return " ".join(cleaned_words)


def remove_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove CSV-generated unnamed index columns."""
    unnamed = [c for c in df.columns if c.lower().startswith("unnamed")]
    return df.drop(columns=unnamed)


def validate_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")


def clean_user_credit_card_data(df: pd.DataFrame) -> pd.DataFrame:
    """Main cleaning pipeline."""
    df = df.copy()

    # Remove junk columns
    df = remove_unnamed_columns(df)

    # Validate schema
    validate_required_columns(df)

    # Normalize strings
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["issuing_bank"] = df["issuing_bank"].astype(str).str.strip()

    # Clean issuing bank names
    df["issuing_bank"] = df["issuing_bank"].apply(
        BankCleaner.clean_bank_name
    )

    # Replace empty values with NULL
    df = df.replace({"": None, "nan": None})

    return df

# =========================================================
#                COPY LOADING FUNCTION
# =========================================================
def write_to_staging(df: pd.DataFrame) -> None:
    engine = create_engine(DB_URI)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Ensure schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")

    # Reset table
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{STAGING_TABLE};")

    # Create table
    columns_sql = ", ".join(
        f"{col} {dtype}" for col, dtype in DTYPE_MAPPING.items()
    )

    cursor.execute(
        f"""
        CREATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE} (
            {columns_sql}
        );
        """
    )

    # COPY load
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
        print(f"📥 Loading source table: {table}")
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        df = clean_user_credit_card_data(df)
        frames.append(df)

    final_df = pd.concat(frames, ignore_index=True)
    write_to_staging(final_df)

    print("🎉 User credit card ELT pipeline completed successfully")

if __name__ == "__main__":
    main()