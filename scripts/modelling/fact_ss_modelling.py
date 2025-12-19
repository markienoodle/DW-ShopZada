import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
import pandas as pd

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
# FACT TABLE LOADING QUERIES
# =====================================================

FACT_TABLE_LOADS = {
    'fact_orders': {
        'description': 'Load orders with current dimension SKs',
        'dependencies': ['dim_user', 'dim_merchant', 'dim_staff', 'dim_campaign'],
        'query': """
            INSERT INTO fact_orders (
                order_id, user_sk, merchant_sk, staff_sk, campaign_sk, 
                date_sk, estimated_arrival, delay_in_days, campaign_availed
            )
            SELECT 
                t2.order_id,
                du.user_sk,
                dm.merchant_sk,
                ds.staff_sk,
                dc.campaign_sk,
                t2.date_sk,
                t2.estimated_arrival,
                t2.delay_in_days,
                t2.campaign_availed
            FROM t2_schema.fact_orders t2
            LEFT JOIN dim_user du 
                ON t2.user_sk = du.user_id
                AND du.is_current = TRUE
            LEFT JOIN dim_merchant dm 
                ON t2.merchant_sk = dm.merchant_id
                AND dm.is_current = TRUE
            LEFT JOIN dim_staff ds 
                ON t2.staff_sk = ds.staff_id
                AND ds.is_current = TRUE
            LEFT JOIN dim_campaign dc 
                ON t2.campaign_sk = dc.campaign_id
                AND dc.is_current = TRUE
            WHERE NOT EXISTS (
                SELECT 1 
                FROM fact_orders fo 
                WHERE fo.order_id = t2.order_id
            )
        """
    },
    'fact_line_items': {
        'description': 'Load line items with current dimension SKs',
        'dependencies': ['fact_orders', 'dim_product'],
        'query': """
            INSERT INTO fact_line_items (
                line_item_id, order_sk, product_sk, merchant_sk, 
                user_sk, date_sk, price, quantity
            )
            SELECT 
                t2.line_item_id,
                fo.order_sk,
                dp.product_sk,
                fo.merchant_sk,
                fo.user_sk,
                fo.date_sk,
                t2.price,
                t2.quantity
            FROM t2_schema.fact_line_items t2
            INNER JOIN fact_orders fo 
                ON t2.order_sk = fo.order_id
            LEFT JOIN dim_product dp 
                ON t2.product_sk = dp.product_id
                AND dp.is_current = TRUE
            WHERE NOT EXISTS (
                SELECT 1 
                FROM fact_line_items fli 
                WHERE fli.order_sk = fo.order_sk 
                AND fli.line_item_id = t2.line_item_id
            )
        """
    }
}

# =====================================================
# VERIFICATION QUERIES
# =====================================================

VERIFICATION_QUERIES = {
    'dimension_counts': {
        'description': 'Check dimension record counts by currency status',
        'query': """
            SELECT 
                'dim_location' as table_name,
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_count,
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) as historical_count,
                COUNT(*) as total_count
            FROM dim_location
            UNION ALL
            SELECT 
                'dim_user',
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END),
                COUNT(*)
            FROM dim_user
            UNION ALL
            SELECT 
                'dim_issuing_bank',
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END),
                COUNT(*)
            FROM dim_issuing_bank
            UNION ALL
            SELECT 
                'dim_product',
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END),
                COUNT(*)
            FROM dim_product
            UNION ALL
            SELECT 
                'dim_merchant',
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END),
                COUNT(*)
            FROM dim_merchant
            UNION ALL
            SELECT 
                'dim_staff',
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END),
                COUNT(*)
            FROM dim_staff
            UNION ALL
            SELECT 
                'dim_campaign',
                SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END),
                COUNT(*)
            FROM dim_campaign
        """
    },
    'fact_counts': {
        'description': 'Check fact table counts',
        'query': """
            SELECT 'fact_orders' as table_name, COUNT(*) as row_count 
            FROM fact_orders
            UNION ALL
            SELECT 'fact_line_items', COUNT(*) 
            FROM fact_line_items
        """
    },
    'orphaned_records': {
        'description': 'Check for orphaned records in fact tables',
        'query': """
            SELECT 
                'Orders with NULL user_sk' as check_name,
                COUNT(*) as count
            FROM fact_orders
            WHERE user_sk IS NULL
            UNION ALL
            SELECT 
                'Orders with NULL merchant_sk',
                COUNT(*)
            FROM fact_orders
            WHERE merchant_sk IS NULL
            UNION ALL
            SELECT 
                'Line items with NULL product_sk',
                COUNT(*)
            FROM fact_line_items
            WHERE product_sk IS NULL
        """
    },
    'duplicate_current_records': {
        'description': 'Check for multiple current records (data quality issue)',
        'query': """
            SELECT 
                'Users with multiple current records' as check_name,
                COUNT(*) as count
            FROM (
                SELECT user_id, COUNT(*) as cnt
                FROM dim_user
                WHERE is_current = TRUE
                GROUP BY user_id
                HAVING COUNT(*) > 1
            ) dup
            UNION ALL
            SELECT 
                'Products with multiple current records',
                COUNT(*)
            FROM (
                SELECT product_id, COUNT(*) as cnt
                FROM dim_product
                WHERE is_current = TRUE
                GROUP BY product_id
                HAVING COUNT(*) > 1
            ) dup
            UNION ALL
            SELECT 
                'Merchants with multiple current records',
                COUNT(*)
            FROM (
                SELECT merchant_id, COUNT(*) as cnt
                FROM dim_merchant
                WHERE is_current = TRUE
                GROUP BY merchant_id
                HAVING COUNT(*) > 1
            ) dup
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

def count_records(conn, table_name):
    """Count records in a table"""
    try:
        query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")
        result = conn.execute(query)
        count = result.fetchone()[0]
        return count
    except Exception as e:
        logger.warning(f"Could not count records in {table_name}: {e}")
        return 0

def table_exists(conn, table_name):
    """Check if table exists"""
    try:
        query = text(f"SELECT 1 FROM {table_name} LIMIT 1")
        conn.execute(query)
        return True
    except Exception:
        return False

def truncate_fact_table(conn, table_name):
    """Truncate a fact table"""
    try:
        query = text(f"TRUNCATE TABLE {table_name}")
        conn.execute(query)
        conn.commit()
        logger.info(f"✓ Truncated {table_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to truncate {table_name}: {e}")
        return False

# =====================================================
# FACT TABLE LOADING FUNCTIONS
# =====================================================

def load_fact_table(conn, fact_table_name, truncate=False):
    """Load a single fact table"""
    
    if fact_table_name not in FACT_TABLE_LOADS:
        logger.error(f"Fact table {fact_table_name} not found in configuration")
        return False
    
    config = FACT_TABLE_LOADS[fact_table_name]
    
    try:
        logger.info(f"Loading {fact_table_name}: {config['description']}")
        
        # Optional: truncate existing data
        if truncate:
            truncate_fact_table(conn, fact_table_name)
        
        # Execute insert query
        query = text(config['query'])
        result = conn.execute(query)
        conn.commit()
        
        # Count loaded records
        record_count = count_records(conn, fact_table_name)
        logger.info(f"✓ {fact_table_name} loaded successfully ({record_count} total records)")
        
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to load {fact_table_name}: {e}")
        return False

def load_all_fact_tables(truncate=False):
    """Load all fact tables in correct dependency order"""
    
    processing_order = [
        'fact_orders',          # No fact table dependencies
        'fact_line_items'       # Depends on fact_orders
    ]
    
    logger.info("=" * 70)
    logger.info("FACT TABLES LOADING PROCESS")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Database: {DB_NAME} @ {DB_HOST}")
    logger.info(f"Truncate existing: {truncate}")
    logger.info("=" * 70)
    
    conn = get_db_connection()
    results = {}
    
    for fact_table in processing_order:
        results[fact_table] = load_fact_table(conn, fact_table, truncate=truncate)
    
    conn.close()
    
    # Print summary
    logger.info("\n" + "=" * 70)
    logger.info("LOADING SUMMARY")
    logger.info("=" * 70)
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for fact_table, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{fact_table:25} {status}")
    
    logger.info("=" * 70)
    logger.info(f"Result: {success_count}/{total_count} fact tables loaded successfully")
    logger.info("=" * 70)
    
    return all(results.values())

# =====================================================
# VERIFICATION FUNCTIONS
# =====================================================

def run_verification_check(conn, check_name):
    """Run a single verification query"""
    
    if check_name not in VERIFICATION_QUERIES:
        logger.error(f"Verification check {check_name} not found")
        return None
    
    config = VERIFICATION_QUERIES[check_name]
    
    try:
        logger.info(f"\n[{check_name}] {config['description']}")
        logger.info("-" * 70)
        
        query = text(config['query'])
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Display results
        print(df.to_string(index=False))
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to run verification check {check_name}: {e}")
        return None

def run_all_verifications():
    """Run all verification checks"""
    
    logger.info("\n" + "=" * 70)
    logger.info("DATA QUALITY VERIFICATION CHECKS")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    conn = get_db_connection()
    
    check_order = [
        'dimension_counts',
        'fact_counts',
        'orphaned_records',
        'duplicate_current_records'
    ]
    
    results = {}
    for check in check_order:
        results[check] = run_verification_check(conn, check)
    
    conn.close()
    
    # Summary of issues found
    logger.info("\n" + "=" * 70)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 70)
    
    issues_found = False
    
    # Check orphaned records
    if results.get('orphaned_records') is not None:
        orphaned_df = results['orphaned_records']
        orphaned_count = orphaned_df[orphaned_df['count'] > 0].shape[0]
        if orphaned_count > 0:
            logger.warning(f"⚠ Found {orphaned_count} types of orphaned records")
            issues_found = True
    
    # Check duplicate current records
    if results.get('duplicate_current_records') is not None:
        duplicates_df = results['duplicate_current_records']
        duplicates_count = duplicates_df[duplicates_df['count'] > 0].shape[0]
        if duplicates_count > 0:
            logger.warning(f"⚠ Found {duplicates_count} types of duplicate current records")
            issues_found = True
    
    if not issues_found:
        logger.info("✓ All data quality checks passed - no issues found")
    
    logger.info("=" * 70)

# =====================================================
# MAIN EXECUTION
# =====================================================

if __name__ == "__main__":
    import sys
    
    # Test connection first
    if not test_connection():
        logger.error("Cannot proceed without database connection")
        sys.exit(1)
    
    # Parse arguments
    truncate = False
    skip_verification = False
    
    if '--truncate' in sys.argv:
        truncate = True
        logger.warning("TRUNCATE mode enabled - existing fact data will be removed")
    
    if '--skip-verification' in sys.argv:
        skip_verification = True
        logger.info("Verification checks will be skipped")
    
    # Load all fact tables
    success = load_all_fact_tables(truncate=truncate)
    
    # Run verification checks
    if success and not skip_verification:
        run_all_verifications()
    
    sys.exit(0 if success else 1)