import os
from sqlalchemy import create_engine, text
import logging

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =====================================================
# DATABASE CONFIG
# =====================================================
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "shopzada_dwh")

if os.path.exists("/opt/airflow"):
    DB_HOST = "shopzada-postgres-db"

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI, echo=False)

# =====================================================
# DIMENSION CONFIG FOR SCD TYPE 2
# =====================================================
DIMENSION_CONFIGS = {
    "dim_location": {
        "natural_key": "location_sk",
        "changed_columns": ["street", "city", "state", "country"],
        "source_table": "staging2_schema.dim_location_t2",
        "target_table": "dim_location",
        "sk_column": "location_sk"
    },
    "dim_user": {
        "natural_key": "user_id",
        "changed_columns": ["name", "birthdate", "gender", "device_address",
                            "user_type", "job_title", "job_level", "location_sk"],
        "source_table": "staging2_schema.dim_user_t2",
        "target_table": "dim_user",
        "sk_column": "user_sk"
    },
    "dim_product": {
        "natural_key": "product_id",
        "changed_columns": ["product_name", "product_type", "price"],
        "source_table": "staging2_schema.dim_product_t2",
        "target_table": "dim_product",
        "sk_column": "product_sk"
    },
    "dim_merchant": {
        "natural_key": "merchant_id",
        "changed_columns": ["name", "contact_number", "location_sk"],
        "source_table": "staging2_schema.dim_merchant_t2",
        "target_table": "dim_merchant",
        "sk_column": "merchant_sk"
    },
    "dim_staff": {
        "natural_key": "staff_id",
        "changed_columns": ["name", "job_level", "contact_number", "location_sk"],
        "source_table": "staging2_schema.dim_staff_t2",
        "target_table": "dim_staff",
        "sk_column": "staff_sk"
    },
    "dim_campaign": {
        "natural_key": "campaign_id",
        "changed_columns": ["campaign_name", "campaign_description", "discount"],
        "source_table": "staging2_schema.dim_campaign_t2",
        "target_table": "dim_campaign",
        "sk_column": "campaign_sk"
    },
    "dim_issuing_bank": {
        "natural_key": "issuing_bank_sk",
        "changed_columns": ["issuing_bank", "user_id"],
        "source_table": "staging2_schema.dim_issuing_bank_t2",
        "target_table": "dim_issuing_bank",
        "sk_column": "issuing_bank_sk"
    }
}

# Prepend star_schema to all target tables
for config in DIMENSION_CONFIGS.values():
    config["target_table"] = f"star_schema.{config['target_table']}"

# =====================================================
# CREATE STAR_SCHEMA TABLES IF NOT EXISTS
# =====================================================
def create_scd_tables():
    logger.info("Creating star_schema tables if they do not exist...")
    with engine.begin() as conn:
        table_creations = {
            "dim_location": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_location (
                    location_sk SERIAL PRIMARY KEY,
                    street TEXT,
                    city TEXT,
                    state TEXT,
                    country TEXT,
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """,
            "dim_user": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_user (
                    user_sk SERIAL PRIMARY KEY,
                    user_id TEXT,
                    name TEXT,
                    birthdate DATE,
                    gender TEXT,
                    device_address TEXT,
                    user_type TEXT,
                    job_title TEXT,
                    job_level TEXT,
                    location_sk INT DEFAULT 0,
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """,
            "dim_product": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_product (
                    product_sk SERIAL PRIMARY KEY,
                    product_id TEXT,
                    product_name TEXT,
                    product_type TEXT,
                    price DECIMAL(10,2),
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """,
            "dim_merchant": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_merchant (
                    merchant_sk SERIAL PRIMARY KEY,
                    merchant_id TEXT,
                    name TEXT,
                    contact_number TEXT,
                    location_sk INT DEFAULT 0,
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """,
            "dim_staff": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_staff (
                    staff_sk SERIAL PRIMARY KEY,
                    staff_id TEXT,
                    name TEXT,
                    job_level TEXT,
                    contact_number TEXT,
                    location_sk INT DEFAULT 0,
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """,
            "dim_campaign": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_campaign (
                    campaign_sk SERIAL PRIMARY KEY,
                    campaign_id TEXT,
                    campaign_name TEXT,
                    campaign_description TEXT,
                    discount DECIMAL(5,2),
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """,
            "dim_issuing_bank": f"""
                CREATE TABLE IF NOT EXISTS star_schema.dim_issuing_bank (
                    issuing_bank_sk SERIAL PRIMARY KEY,
                    issuing_bank TEXT,
                    user_id TEXT,
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN DEFAULT TRUE
                );
            """
        }
        for name, ddl in table_creations.items():
            logger.info(f"Ensuring table exists: {name}")
            conn.execute(text(ddl))

# =====================================================
# CORE SCD TYPE 2 FUNCTIONS
# =====================================================
def process_dimension_scd(conn, dimension_name, config):
    """Expire old records and insert new SCD Type 2 records with SK fallback to 0"""
    natural_key = config["natural_key"]
    changed_cols = config["changed_columns"]
    source_table = config["source_table"]
    target_table = config["target_table"]
    sk_col = config.get("sk_column", None)

    logger.info(f"Processing SCD Type 2 for {dimension_name}...")

    # Add SK fallback to 0 for all dimensions
    if dimension_name in ["dim_user", "dim_merchant", "dim_staff"]:
        source_table_with_sk = f"""
            (SELECT t2.*,
                    COALESCE(dl.location_sk, 0) AS location_sk
             FROM {source_table} t2
             LEFT JOIN star_schema.dim_location dl
               ON t2.street = dl.street
              AND t2.city   = dl.city
              AND t2.state  = dl.state
              AND t2.country= dl.country)
        """
    else:
        source_table_with_sk = f"""
            (SELECT t2.*, 0 AS {sk_col} FROM {source_table} t2)
        """ if sk_col else source_table

    # Build composite key condition if needed
    if dimension_name == "dim_user":
        key_condition = "t2.user_id = dim.user_id"
    elif dimension_name == "dim_staff":
        key_condition = "t2.staff_id = dim.staff_id"
    elif dimension_name == "dim_merchant":
        key_condition = "t2.merchant_id = dim.merchant_id"
    else:
        key_condition = f"t2.{natural_key} = dim.{natural_key}"

    # Detect changed columns
    changes_condition = " OR ".join([f"t2.{col} IS DISTINCT FROM dim.{col}" for col in changed_cols])

    # Step 1: Expire old records
    expire_sql = f"""
        UPDATE {target_table} dim
        SET is_current = FALSE,
            valid_to = CURRENT_DATE
        FROM {source_table_with_sk} t2
        WHERE {key_condition}
          AND dim.is_current = TRUE
          AND ({changes_condition});
    """

    # Step 2: Insert new or changed records
    columns_sql = ", ".join(changed_cols + ["valid_from", "is_current", "valid_to"])
    insert_sql = f"""
        INSERT INTO {target_table} ({columns_sql})
        SELECT {', '.join([f't2.{col}' for col in changed_cols])}, 
            CURRENT_DATE, 
            TRUE, 
            NULL
        FROM {source_table_with_sk} t2
        LEFT JOIN {target_table} dim
        ON {key_condition} AND dim.is_current = TRUE
        WHERE dim.{natural_key} IS NULL OR ({changes_condition});
    """


    try:
        conn.execute(text(expire_sql))
        conn.execute(text(insert_sql))
        logger.info(f"✓ {dimension_name} processed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to process {dimension_name}: {e}")
        raise

# =====================================================
# RUNNER
# =====================================================
def run_all_dimensions():
    """Create tables and process all dimensions in dependency order"""
    create_scd_tables()
    processing_order = [
        "dim_location",
        "dim_user",
        "dim_product",
        "dim_merchant",
        "dim_staff",
        "dim_campaign",
        "dim_issuing_bank"
    ]

    results = {}
    with engine.begin() as conn:  # handles commit/rollback
        for dim in processing_order:
            config = DIMENSION_CONFIGS[dim]
            results[dim] = process_dimension_scd(conn, dim, config)

    logger.info("\n" + "="*60)
    logger.info("SCD Type 2 Transformation Summary")
    logger.info("="*60)
    for dim, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{dim}: {status}")
    return all(results.values())

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    try:
        run_all_dimensions()
        logger.info("All dimensions processed successfully")
    except Exception as e:
        logger.error(f"SCD Type 2 process failed: {e}")
        exit(1)
