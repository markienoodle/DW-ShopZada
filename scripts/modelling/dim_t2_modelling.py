import os
from sqlalchemy import create_engine, text
import logging

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
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
# T2 STAR SCHEMA TARGET TABLES (no PKs, no unique)
# =====================================================
TABLE_SCHEMAS = {
    "dim_location_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_location_t2 (
            street TEXT,
            city TEXT,
            state TEXT,
            country TEXT
        );
    """,
    "dim_user_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_user_t2 (
            user_id TEXT,
            name TEXT,
            birthdate DATE,
            gender TEXT,
            device_address TEXT,
            creation_date DATE,
            user_type TEXT,
            job_title TEXT,
            job_level TEXT,
            street TEXT,
            city TEXT,
            state TEXT,
            country TEXT
        );
    """,
    "dim_product_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_product_t2 (
            product_id TEXT,
            product_name TEXT,
            product_type TEXT,
            price DECIMAL(10,2)
        );
    """,
    "dim_merchant_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_merchant_t2 (
            merchant_id TEXT,
            name TEXT,
            contact_number TEXT,
            creation_date TIMESTAMP,
            street TEXT,
            city TEXT,
            state TEXT,
            country TEXT
        );
    """,
    "dim_staff_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_staff_t2 (
            staff_id TEXT,
            name TEXT,
            job_level TEXT,
            contact_number TEXT,
            creation_date TIMESTAMP,
            street TEXT,
            city TEXT,
            state TEXT,
            country TEXT
        );
    """,
    "dim_campaign_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_campaign_t2 (
            campaign_id TEXT,
            campaign_name TEXT,
            campaign_description TEXT,
            discount DECIMAL(5,2)
        );
    """,
    "dim_issuing_bank_t2": """
        CREATE TABLE IF NOT EXISTS staging2_schema.dim_issuing_bank_t2 (
            user_id TEXT,
            issuing_bank TEXT
        );
    """
}

# =====================================================
# T2 SNAPSHOT LOAD QUERIES (all duplicates allowed)
# =====================================================
DIMENSION_LOADS = {
    "dim_location_t2": """
        TRUNCATE TABLE staging2_schema.dim_location_t2;
        INSERT INTO staging2_schema.dim_location_t2 (street, city, state, country)
        SELECT street, city, state, country
        FROM (
            SELECT street, city, state, country FROM staging1_schema.user_data_deduped
            UNION ALL
            SELECT street, city, state, country FROM staging1_schema.merchant_data_deduped
            UNION ALL
            SELECT street, city, state, country FROM staging1_schema.staff_data_deduped
        ) l
        WHERE street IS NOT NULL;
    """,

    "dim_user_t2": """
        TRUNCATE TABLE staging2_schema.dim_user_t2;
        INSERT INTO staging2_schema.dim_user_t2 (
            user_id, name, birthdate, gender, device_address,
            creation_date, user_type, job_title, job_level,
            street, city, state, country
        )
        SELECT
            ud.user_id,
            ud.name,
            ud.birthdate::DATE,
            ud.gender,
            ud.device_address,
            ud.creation_date,
            ud.user_type,
            uj.job_title,
            uj.job_level,
            ud.street,
            ud.city,
            ud.state,
            ud.country
        FROM staging1_schema.user_data_deduped ud
        LEFT JOIN staging1_schema.user_job_deduped uj
            ON ud.user_id = uj.user_id;
    """,

    "dim_product_t2": """
        TRUNCATE TABLE staging2_schema.dim_product_t2;
        INSERT INTO staging2_schema.dim_product_t2 (product_id, product_name, product_type, price)
        SELECT product_id, product_name, product_type, price
        FROM staging1_schema.product_list_deduped;
    """,

    "dim_merchant_t2": """
        TRUNCATE TABLE staging2_schema.dim_merchant_t2;
        INSERT INTO staging2_schema.dim_merchant_t2 (
            merchant_id, name, contact_number, creation_date,
            street, city, state, country
        )
        SELECT
            md.merchant_id,
            md.name,
            md.contact_number,
            md.creation_date,
            md.street,
            md.city,
            md.state,
            md.country
        FROM staging1_schema.merchant_data_deduped md;
    """,

    "dim_staff_t2": """
        TRUNCATE TABLE staging2_schema.dim_staff_t2;
        INSERT INTO staging2_schema.dim_staff_t2 (
            staff_id, name, job_level, contact_number, creation_date,
            street, city, state, country
        )
        SELECT
            sd.staff_id,
            sd.name,
            sd.job_level,
            sd.contact_number,
            sd.creation_date,
            sd.street,
            sd.city,
            sd.state,
            sd.country
        FROM staging1_schema.staff_data_deduped sd;
    """,

    "dim_campaign_t2": """
        TRUNCATE TABLE staging2_schema.dim_campaign_t2;
        INSERT INTO staging2_schema.dim_campaign_t2 (
            campaign_id, campaign_name, campaign_description, discount
        )
        SELECT
            campaign_id,
            campaign_name,
            campaign_description,
            discount
        FROM staging1_schema.campaign_data_deduped;
    """,

    "dim_issuing_bank_t2": """
        TRUNCATE TABLE staging2_schema.dim_issuing_bank_t2;
        INSERT INTO staging2_schema.dim_issuing_bank_t2 (user_id, issuing_bank)
        SELECT user_id, issuing_bank
        FROM staging1_schema.user_credit_card_deduped;
    """
}

# =====================================================
# RUNNER
# =====================================================
def create_tables():
    with engine.begin() as conn:
        for name, ddl in TABLE_SCHEMAS.items():
            logger.info(f"Ensuring table exists: {name}")
            conn.execute(text(ddl))

def load_dimensions():
    load_order = [
        "dim_location_t2",
        "dim_product_t2",
        "dim_campaign_t2",
        "dim_user_t2",
        "dim_merchant_t2",
        "dim_staff_t2",
        "dim_issuing_bank_t2",
    ]

    for dim in load_order:
        logger.info(f"Loading {dim}")
        with engine.begin() as conn:
            conn.execute(text(DIMENSION_LOADS[dim]))

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    create_tables()
    load_dimensions()
    logger.info("✓ T2 star schema snapshot completed successfully")
