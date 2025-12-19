import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
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

# Create engine and session factory
engine = create_engine(DB_URI, echo=False)
SessionLocal = sessionmaker(bind=engine)

# =====================================================
# SCD TYPE 2 TRANSFORMATION QUERIES
# =====================================================

DIMENSION_CONFIGS = {
    'dim_location': {
        'natural_key': 'location_sk',
        'changed_columns': ['street', 'city', 'state', 'country'],
        'source_table': 't2_schema.dim_location',
        'target_table': 'dim_location'
    },
    'dim_user': {
        'natural_key': 'user_id',
        'changed_columns': ['name', 'birthdate', 'gender', 'device_address', 'user_type', 'job_title', 'job_level', 'location_sk'],
        'source_table': 't2_schema.dim_user',
        'target_table': 'dim_user',
        'foreign_keys': {'location_sk': ('dim_location', 'location_sk')}
    },
    'dim_product': {
        'natural_key': 'product_id',
        'changed_columns': ['product_name', 'product_type', 'price'],
        'source_table': 't2_schema.dim_product',
        'target_table': 'dim_product'
    },
    'dim_merchant': {
        'natural_key': 'merchant_id',
        'changed_columns': ['name', 'contact_number', 'location_sk'],
        'source_table': 't2_schema.dim_merchant',
        'target_table': 'dim_merchant',
        'foreign_keys': {'location_sk': ('dim_location', 'location_sk')}
    },
    'dim_issuing_bank': {
        'natural_key': 'bank_sk',
        'changed_columns': ['issuing_bank', 'user_sk'],
        'source_table': 't2_schema.dim_issuing_bank',
        'target_table': 'dim_issuing_bank',
        'foreign_keys': {'user_sk': ('dim_user', 'user_sk')}
    },
    'dim_staff': {
        'natural_key': 'staff_id',
        'changed_columns': ['name', 'job_level', 'contact_number', 'location_sk'],
        'source_table': 't2_schema.dim_staff',
        'target_table': 'dim_staff',
        'foreign_keys': {'location_sk': ('dim_location', 'location_sk')}
    },
    'dim_campaign': {
        'natural_key': 'campaign_id',
        'changed_columns': ['campaign_name', 'campaign_description', 'discount'],
        'source_table': 't2_schema.dim_campaign',
        'target_table': 'dim_campaign'
    }
}

# =====================================================
# TRANSFORMATION FUNCTIONS
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

def expire_changed_records(conn, dimension_name, config):
    """Expire old records when changes are detected"""
    natural_key = config['natural_key']
    changed_cols = config['changed_columns']
    source_table = config['source_table']
    target_table = config['target_table']
    
    # Build WHERE clause for changed columns
    where_conditions = ' OR '.join(
        [f"t2.{col} != dim.{col}" for col in changed_cols]
    )
    
    query = text(f"""
        UPDATE {target_table}
        SET is_current = FALSE,
            valid_to = CURRENT_DATE
        WHERE {natural_key} IN (
            SELECT t2.{natural_key}
            FROM {source_table} t2
            INNER JOIN {target_table} dim 
                ON t2.{natural_key} = dim.{natural_key}
                AND dim.is_current = TRUE
            WHERE {where_conditions}
        )
        AND is_current = TRUE
    """)
    
    try:
        conn.execute(query)
        conn.commit()
        logger.info(f"✓ Expired changed records in {target_table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to expire records in {target_table}: {e}")
        raise

def insert_new_changed_records(conn, dimension_name, config):
    """Insert new and changed records"""
    natural_key = config['natural_key']
    source_table = config['source_table']
    target_table = config['target_table']
    foreign_keys = config.get('foreign_keys', {})
    
    # Build column list and SELECT clause
    columns = ', '.join([col for col in dir(config) if not col.startswith('_')])
    
    # Start with basic SELECT
    select_clause = f"SELECT t2.* FROM {source_table} t2"
    
    # Add foreign key joins
    joins = ""
    for fk_col, (fk_table, fk_key) in foreign_keys.items():
        joins += f"\nLEFT JOIN {fk_table} fk_{fk_col} ON t2.{fk_col} = fk_{fk_key} AND fk_{fk_col}.is_current = TRUE"
    
    query = text(f"""
        INSERT INTO {target_table}
        SELECT 
            t2.*,
            TRUE as is_current,
            CURRENT_DATE as valid_from,
            NULL as valid_to
        FROM {source_table} t2
        {joins}
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {target_table} dim 
            WHERE dim.{natural_key} = t2.{natural_key} 
            AND dim.is_current = TRUE
        )
        OR EXISTS (
            SELECT 1
            FROM {target_table} dim
            WHERE dim.{natural_key} = t2.{natural_key}
            AND dim.is_current = FALSE
            AND dim.valid_to = CURRENT_DATE
        )
    """)
    
    try:
        conn.execute(query)
        conn.commit()
        logger.info(f"✓ Inserted new/changed records into {target_table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to insert records into {target_table}: {e}")
        raise

def process_scd_type2(dimension_name):
    """Main SCD Type 2 processing for a dimension"""
    if dimension_name not in DIMENSION_CONFIGS:
        logger.error(f"Dimension {dimension_name} not found in configuration")
        return False
    
    config = DIMENSION_CONFIGS[dimension_name]
    
    try:
        conn = get_db_connection()
        logger.info(f"Processing SCD Type 2 for {dimension_name}...")
        
        # Step 1: Expire changed records
        expire_changed_records(conn, dimension_name, config)
        
        # Step 2: Insert new and changed records
        insert_new_changed_records(conn, dimension_name, config)
        
        logger.info(f"✓ Successfully processed {dimension_name}")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Error processing {dimension_name}: {e}")
        return False

def process_all_dimensions():
    """Process all dimensions in the correct order"""
    # Process in dependency order
    processing_order = [
        'dim_location',      # No dependencies
        'dim_user',          # Depends on dim_location
        'dim_product',       # No dependencies
        'dim_merchant',      # Depends on dim_location
        'dim_issuing_bank',  # Depends on dim_user
        'dim_staff',         # Depends on dim_location
        'dim_campaign'       # No dependencies
    ]
    
    logger.info("=" * 60)
    logger.info("Starting SCD Type 2 Transformation Process")
    logger.info("=" * 60)
    
    results = {}
    for dimension in processing_order:
        results[dimension] = process_scd_type2(dimension)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Transformation Summary")
    logger.info("=" * 60)
    for dimension, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{dimension}: {status}")
    
    return all(results.values())

# =====================================================
# MAIN EXECUTION
# =====================================================

if __name__ == "__main__":
    # Test connection first
    if test_connection():
        # Process all dimensions
        success = process_all_dimensions()
        exit(0 if success else 1)
    else:
        logger.error("Cannot proceed without database connection")
        exit(1)