import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
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

# Create engine
engine = create_engine(DB_URI, echo=False)
SessionLocal = sessionmaker(bind=engine)

# =====================================================
# DIMENSION POPULATION QUERIES
# =====================================================

DIMENSION_INSERTS = {
    'dim_location': {
        'description': 'Combines all unique locations from users, merchants, and staff',
        'query': """
            INSERT INTO dim_location (location_sk, street, city, state, country, is_current, valid_from, valid_to)
            SELECT DISTINCT
                ROW_NUMBER() OVER (ORDER BY street, city, state, country) as location_sk,
                street,
                city,
                state,
                country,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM (
                -- User locations
                SELECT street, city, state, country
                FROM staging1_schema.user_data_deduped
                
                UNION
                
                -- Merchant locations
                SELECT street, city, state, country
                FROM staging1_schema.merchant_data_deduped
                
                UNION
                
                -- Staff locations
                SELECT street, city, state, country
                FROM staging1_schema.staff_data_deduped
            ) all_locations
            WHERE street IS NOT NULL 
              AND city IS NOT NULL 
              AND state IS NOT NULL 
              AND country IS NOT NULL
        """
    },
    'dim_user': {
        'description': 'Combines user_data, user_job, and links to location',
        'query': """
            INSERT INTO dim_user (
                user_sk, user_id, name, birthdate, gender, device_address, 
                creation_date, user_type, job_title, job_level, 
                location_sk, is_current, valid_from, valid_to
            )
            SELECT 
                ud.user_id as user_sk,
                ud.user_id,
                ud.name,
                ud.birthdate::DATE,
                ud.gender,
                ud.device_address,
                ud.creation_date,
                ud.user_type,
                uj.job_title,
                uj.job_level,
                dl.location_sk,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM staging1_schema.user_data_deduped ud
            LEFT JOIN staging1_schema.user_job_deduped uj 
                ON ud.user_id = uj.user_id
            LEFT JOIN dim_location dl 
                ON ud.street = dl.street 
                AND ud.city = dl.city 
                AND ud.state = dl.state 
                AND ud.country = dl.country
                AND dl.is_current = TRUE
        """
    },
    'dim_issuing_bank': {
        'description': 'Links users to their issuing banks',
        'query': """
            INSERT INTO dim_issuing_bank (
                bank_sk, user_sk, issuing_bank, is_current, valid_from, valid_to
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY ucc.user_id, ucc.issuing_bank) as bank_sk,
                ucc.user_id as user_sk,
                ucc.issuing_bank,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM staging1_schema.user_credit_card_deduped ucc
        """
    },
    'dim_product': {
        'description': 'Combines product list with line item data',
        'query': """
            INSERT INTO dim_product (
                product_sk, product_id, product_name, product_type, price, 
                is_current, valid_from, valid_to
            )
            SELECT DISTINCT
                COALESCE(pl.product_id, li.product_id) as product_sk,
                COALESCE(pl.product_id, li.product_id) as product_id,
                COALESCE(pl.product_name, li.product_name) as product_name,
                pl.product_type,
                COALESCE(pl.price, li.price) as price,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM staging1_schema.product_list_deduped pl
            FULL OUTER JOIN staging1_schema.line_item_data_deduped li
                ON pl.product_id = li.product_id
            WHERE COALESCE(pl.product_id, li.product_id) IS NOT NULL
        """
    },
    'dim_merchant': {
        'description': 'Merchant data with location reference',
        'query': """
            INSERT INTO dim_merchant (
                merchant_sk, merchant_id, name, contact_number, creation_date, 
                location_sk, is_current, valid_from, valid_to
            )
            SELECT 
                md.merchant_id as merchant_sk,
                md.merchant_id,
                md.name,
                md.contact_number,
                md.creation_date,
                dl.location_sk,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM staging1_schema.merchant_data_deduped md
            LEFT JOIN dim_location dl 
                ON md.street = dl.street 
                AND md.city = dl.city 
                AND md.state = dl.state 
                AND md.country = dl.country
                AND dl.is_current = TRUE
        """
    },
    'dim_staff': {
        'description': 'Staff data with location reference',
        'query': """
            INSERT INTO dim_staff (
                staff_sk, staff_id, name, job_level, contact_number, 
                creation_date, location_sk, is_current, valid_from, valid_to
            )
            SELECT 
                sd.staff_id as staff_sk,
                sd.staff_id,
                sd.name,
                sd.job_level,
                sd.contact_number,
                sd.creation_date,
                dl.location_sk,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM staging1_schema.staff_data_deduped sd
            LEFT JOIN dim_location dl 
                ON sd.street = dl.street 
                AND sd.city = dl.city 
                AND sd.state = dl.state 
                AND sd.country = dl.country
                AND dl.is_current = TRUE
        """
    },
    'dim_campaign': {
        'description': 'Campaign master data',
        'query': """
            INSERT INTO dim_campaign (
                campaign_sk, campaign_id, campaign_name, campaign_description, 
                discount, is_current, valid_from, valid_to
            )
            SELECT 
                campaign_id as campaign_sk,
                campaign_id,
                campaign_name,
                campaign_description,
                discount,
                TRUE as is_current,
                CURRENT_DATE as valid_from,
                NULL as valid_to
            FROM staging1_schema.campaign_data_deduped
        """
    }
}

# =====================================================
# UTILITY FUNCTIONS
# =====================================================

def get_db_connection():
    """Get database connection"""
    try:
        conn = engine.connect()
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def test_connection():
    """Test database connectivity"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("✓ Database connection successful")
            return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False

def check_staging_tables():
    """Verify that all required staging tables exist"""
    staging_tables = [
        'staging1_schema.user_data_deduped',
        'staging1_schema.user_job_deduped',
        'staging1_schema.user_credit_card_deduped',
        'staging1_schema.merchant_data_deduped',
        'staging1_schema.staff_data_deduped',
        'staging1_schema.product_list_deduped',
        'staging1_schema.line_item_data_deduped',
        'staging1_schema.campaign_data_deduped'
    ]
    
    conn = get_db_connection()
    missing_tables = []
    
    for table in staging_tables:
        try:
            query = text(f"SELECT 1 FROM {table} LIMIT 1")
            conn.execute(query)
        except Exception as e:
            missing_tables.append(table)
            logger.warning(f"⚠ Staging table not found: {table}")
    
    conn.close()
    
    if missing_tables:
        logger.error(f"Missing {len(missing_tables)} staging table(s)")
        return False
    
    logger.info(f"✓ All {len(staging_tables)} staging tables verified")
    return True

def count_records(conn, table_name):
    """Count records in a dimension table"""
    try:
        query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")
        result = conn.execute(query)
        count = result.fetchone()[0]
        return count
    except Exception as e:
        logger.error(f"Failed to count records in {table_name}: {e}")
        return 0

def truncate_dimension(conn, table_name):
    """Truncate a dimension table (optional, for re-runs)"""
    try:
        query = text(f"TRUNCATE TABLE {table_name} CASCADE")
        conn.execute(query)
        conn.commit()
        logger.info(f"✓ Truncated {table_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to truncate {table_name}: {e}")
        return False

# =====================================================
# MAIN POPULATION FUNCTIONS
# =====================================================

def populate_dimension(conn, dimension_name, truncate=False):
    """Populate a single dimension table"""
    
    if dimension_name not in DIMENSION_INSERTS:
        logger.error(f"Dimension {dimension_name} not found in configuration")
        return False
    
    config = DIMENSION_INSERTS[dimension_name]
    
    try:
        logger.info(f"Processing {dimension_name}: {config['description']}")
        
        # Optional: truncate existing data
        if truncate:
            truncate_dimension(conn, dimension_name)
        
        # Execute insert query
        query = text(config['query'])
        result = conn.execute(query)
        conn.commit()
        
        # Count inserted records
        record_count = count_records(conn, dimension_name)
        logger.info(f"✓ {dimension_name} populated successfully ({record_count} records)")
        
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to populate {dimension_name}: {e}")
        return False

def populate_all_dimensions(truncate=False):
    """Populate all dimension tables in correct dependency order"""
    
    # Processing order matters - dimensions with no dependencies first
    processing_order = [
        'dim_location',      # No dependencies - required by dim_user, dim_merchant, dim_staff
        'dim_product',       # No dependencies
        'dim_campaign',      # No dependencies
        'dim_user',          # Depends on dim_location
        'dim_merchant',      # Depends on dim_location
        'dim_staff',         # Depends on dim_location
        'dim_issuing_bank'   # Depends on dim_user (via staging)
    ]
    
    logger.info("=" * 70)
    logger.info("DIMENSION TABLES POPULATION PROCESS")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Database: {DB_NAME} @ {DB_HOST}")
    logger.info(f"Truncate existing: {truncate}")
    logger.info("=" * 70)
    
    conn = get_db_connection()
    results = {}
    
    for dimension in processing_order:
        results[dimension] = populate_dimension(conn, dimension, truncate=truncate)
    
    conn.close()
    
    # Print summary
    logger.info("\n" + "=" * 70)
    logger.info("POPULATION SUMMARY")
    logger.info("=" * 70)
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for dimension, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{dimension:25} {status}")
    
    logger.info("=" * 70)
    logger.info(f"Result: {success_count}/{total_count} dimensions populated successfully")
    logger.info("=" * 70)
    
    return all(results.values())

# =====================================================
# MAIN EXECUTION
# =====================================================

if __name__ == "__main__":
    import sys
    
    # Test connection first
    if not test_connection():
        logger.error("Cannot proceed without database connection")
        sys.exit(1)
    
    # Check staging tables
    if not check_staging_tables():
        logger.warning("Some staging tables are missing, but continuing...")
    
    # Ask about truncation (optional)
    truncate = False
    if len(sys.argv) > 1 and sys.argv[1].lower() == '--truncate':
        truncate = True
        logger.warning("TRUNCATE mode enabled - existing dimension data will be removed")
    
    # Populate all dimensions
    success = populate_all_dimensions(truncate=truncate)
    
    sys.exit(0 if success else 1)