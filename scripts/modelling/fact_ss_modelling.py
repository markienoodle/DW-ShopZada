import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
import pandas as pd

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================================
# DATABASE CONFIGURATION
# =====================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Detect Airflow container
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI, echo=False)
SessionLocal = sessionmaker(bind=engine)

# =====================================================
# SCHEMA
# =====================================================
STAR_SCHEMA = "star_schema"

# =====================================================
# FACT TABLE LOAD QUERIES
# =====================================================
FACT_TABLE_LOADS = {
    'fact_orders': {
        'description': 'Load orders using surrogate keys from dimensions',
        'dependencies': ['dim_user', 'dim_merchant', 'dim_staff', 'dim_campaign'],
        'query': f"""
            INSERT INTO {STAR_SCHEMA}.fact_orders (
                order_id, user_sk, merchant_sk, staff_sk, campaign_sk, 
                date_sk, estimated_arrival, delay_in_days, campaign_availed
            )
            SELECT 
                od.order_id,
                COALESCE(u.user_sk, 0) AS user_sk,
                COALESCE(m.merchant_sk, 0) AS merchant_sk,
                COALESCE(s.staff_sk, 0) AS staff_sk,
                COALESCE(c.campaign_sk, 0) AS campaign_sk,
                TO_CHAR(od.transaction_date, 'YYYYMMDD')::INT AS date_sk,
                od.estimated_arrival,
                od.delay_in_days,
                COALESCE(tcd.availed, FALSE) AS campaign_availed
            FROM staging1_schema.order_data_deduped od
            LEFT JOIN dim_user u
                ON od.user_id = u.user_id
               AND od.user_name = u.name
               AND u.is_current = TRUE
            LEFT JOIN dim_merchant m
                ON od.merchant_id = m.merchant_id
               AND m.is_current = TRUE
            LEFT JOIN dim_staff s
                ON od.staff_id = s.staff_id
               AND od.staff_name = s.name
               AND s.is_current = TRUE
            LEFT JOIN dim_campaign c
                ON od.campaign_id = c.campaign_id
               AND c.is_current = TRUE
            LEFT JOIN staging1_schema.transactional_campaign_data_deduped tcd
                ON od.order_id = tcd.order_id
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {STAR_SCHEMA}.fact_orders fo 
                WHERE fo.order_id = od.order_id
            )
        """
    },
    'fact_line_items': {
        'description': 'Load line items using surrogate keys from dimensions',
        'dependencies': ['fact_orders', 'dim_product'],
        'query': f"""
            INSERT INTO {STAR_SCHEMA}.fact_line_items (
                line_item_id, order_sk, product_sk, merchant_sk, 
                user_sk, staff_sk, date_sk, price, quantity
            )
            SELECT 
                ROW_NUMBER() OVER (PARTITION BY li.order_id ORDER BY li.product_id) AS line_item_id,
                fo.order_id AS order_sk,
                COALESCE(p.product_sk, 0) AS product_sk,
                fo.merchant_sk,
                fo.user_sk,
                fo.staff_sk,
                fo.date_sk,
                li.price,
                li.quantity
            FROM staging1_schema.line_item_data_deduped li
            INNER JOIN {STAR_SCHEMA}.fact_orders fo
                ON li.order_id = fo.order_id
            LEFT JOIN dim_product p
                ON li.product_id = p.product_id
               AND p.is_current = TRUE
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {STAR_SCHEMA}.fact_line_items fli
                WHERE fli.order_sk = fo.order_id
                  AND fli.line_item_id = ROW_NUMBER() OVER (PARTITION BY li.order_id ORDER BY li.product_id)
            )
        """
    }
}

# =====================================================
# UTILITY FUNCTIONS
# =====================================================
def get_db_connection():
    try:
        conn = engine.connect()
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def test_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("✓ Database connection successful")
            return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False

def count_records(conn, table_name):
    try:
        query = text(f"SELECT COUNT(*) as cnt FROM {STAR_SCHEMA}.{table_name}")
        result = conn.execute(query)
        return result.fetchone()[0]
    except Exception as e:
        logger.warning(f"Could not count records in {table_name}: {e}")
        return 0

def truncate_fact_table(conn, table_name):
    try:
        conn.execute(text(f"TRUNCATE TABLE {STAR_SCHEMA}.{table_name} CASCADE"))
        conn.commit()
        logger.info(f"✓ Truncated {STAR_SCHEMA}.{table_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to truncate {STAR_SCHEMA}.{table_name}: {e}")
        return False

# =====================================================
# FACT LOADING FUNCTIONS
# =====================================================
def load_fact_table(conn, fact_table_name, truncate=False):
    if fact_table_name not in FACT_TABLE_LOADS:
        logger.error(f"Fact table {fact_table_name} not found in configuration")
        return False
    config = FACT_TABLE_LOADS[fact_table_name]
    try:
        logger.info(f"Loading {fact_table_name}: {config['description']}")
        if truncate:
            truncate_fact_table(conn, fact_table_name)
        conn.execute(text(config['query']))
        conn.commit()
        record_count = count_records(conn, fact_table_name)
        logger.info(f"✓ {fact_table_name} loaded successfully ({record_count} total records)")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to load {fact_table_name}: {e}")
        return False

def load_all_fact_tables(truncate=False):
    processing_order = ['fact_orders', 'fact_line_items']
    conn = get_db_connection()
    results = {}
    for table in processing_order:
        results[table] = load_fact_table(conn, table, truncate)
    conn.close()
    
    success_count = sum(1 for v in results.values() if v)
    for table, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{table:25} {status}")
    return all(results.values())

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    import sys
    if not test_connection():
        sys.exit(1)
    
    truncate = '--truncate' in sys.argv
    success = load_all_fact_tables(truncate=truncate)
    sys.exit(0 if success else 1)
