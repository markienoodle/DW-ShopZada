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
# FACT TABLE POPULATION QUERIES (USING BUSINESS KEYS)
# =====================================================

FACT_TABLE_POPULATIONS = {
    'fact_orders': {
        'description': 'Combines order data with transactional campaign data',
        'dependencies': [],
        'query': """
            INSERT INTO fact_orders (
                order_sk, order_id, user_sk, merchant_sk, staff_sk, campaign_sk, 
                date_sk, estimated_arrival, delay_in_days, campaign_availed
            )
            SELECT 
                od.order_id as order_sk,
                od.order_id,
                od.user_id as user_sk,
                od.merchant_id as merchant_sk,
                od.staff_id as staff_sk,
                tcd.campaign_id as campaign_sk,
                TO_CHAR(od.transaction_date, 'YYYYMMDD')::INT as date_sk,
                od.estimated_arrival::DATE as estimated_arrival,
                od.delay_in_days,
                COALESCE(tcd.availed, FALSE) as campaign_availed
            FROM staging1_schema.order_data_deduped od
            LEFT JOIN staging1_schema.transactional_campaign_data_deduped tcd
                ON od.order_id = tcd.order_id
        """
    },
    'fact_line_items': {
        'description': 'Line items linked to orders, products, and dimensions',
        'dependencies': ['fact_orders'],
        'query': """
            INSERT INTO fact_line_items (
                line_item_sk, line_item_id, order_sk, product_sk, merchant_sk, 
                user_sk, date_sk, price, quantity
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY li.order_id, li.product_id) as line_item_sk,
                ROW_NUMBER() OVER (PARTITION BY li.order_id ORDER BY li.product_id) as line_item_id,
                li.order_id as order_sk,
                li.product_id as product_sk,
                fo.merchant_sk,
                fo.user_sk,
                fo.date_sk,
                li.price,
                li.quantity
            FROM staging1_schema.line_item_data_deduped li
            INNER JOIN fact_orders fo 
                ON li.order_id = fo.order_id
        """
    }
}

# =====================================================
# VERIFICATION QUERIES
# =====================================================

VERIFICATION_QUERIES = {
    'all_table_counts': {
        'description': 'Check row counts for all dimensions and facts',
        'query': """
            SELECT 'dim_location' as table_name, COUNT(*) as row_count FROM dim_location
            UNION ALL
            SELECT 'dim_user', COUNT(*) FROM dim_user
            UNION ALL
            SELECT 'dim_issuing_bank', COUNT(*) FROM dim_issuing_bank
            UNION ALL
            SELECT 'dim_product', COUNT(*) FROM dim_product
            UNION ALL
            SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
            UNION ALL
            SELECT 'dim_staff', COUNT(*) FROM dim_staff
            UNION ALL
            SELECT 'dim_campaign', COUNT(*) FROM dim_campaign
            UNION ALL
            SELECT 'fact_orders', COUNT(*) FROM fact_orders
            UNION ALL
            SELECT 'fact_line_items', COUNT(*) FROM fact_line_items
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
                'Orders with NULL staff_sk',
                COUNT(*)
            FROM fact_orders
            WHERE staff_sk IS NULL
            UNION ALL
            SELECT 
                'Line items with NULL product_sk',
                COUNT(*)
            FROM fact_line_items
            WHERE product_sk IS NULL
        """
    },
    'fact_data_sample': {
        'description': 'Sample of loaded fact data',
        'query': """
            SELECT 
                'fact_orders sample' as source,
                order_id::TEXT,
                user_sk::TEXT as dimension_fk,
                delay_in_days::TEXT as metric,
                campaign_availed::TEXT as attribute
            FROM fact_orders
            LIMIT 5
            UNION ALL
            SELECT 
                'fact_line_items sample',
                order_sk::TEXT,
                product_sk::TEXT,
                quantity::TEXT,
                price::TEXT
            FROM fact_line_items
            LIMIT 5
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

def check_staging_tables():
    """Verify that all required staging tables exist"""
    staging_tables = [
        'staging1_schema.order_data_deduped',
        'staging1_schema.transactional_campaign_data_deduped',
        'staging1_schema.line_item_data_deduped'
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

def truncate_fact_table(conn, table_name):
    """Truncate a fact table"""
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
# FACT TABLE POPULATION FUNCTIONS
# =====================================================

def populate_fact_table(conn, fact_table_name, truncate=False):
    """Populate a single fact table"""
    
    if fact_table_name not in FACT_TABLE_POPULATIONS:
        logger.error(f"Fact table {fact_table_name} not found in configuration")
        return False
    
    config = FACT_TABLE_POPULATIONS[fact_table_name]
    
    try:
        logger.info(f"Populating {fact_table_name}: {config['description']}")
        
        # Optional: truncate existing data
        if truncate:
            truncate_fact_table(conn, fact_table_name)
        
        # Execute insert query
        query = text(config['query'])
        result = conn.execute(query)
        conn.commit()
        
        # Count inserted records
        record_count = count_records(conn, fact_table_name)
        logger.info(f"✓ {fact_table_name} populated successfully ({record_count} total records)")
        
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to populate {fact_table_name}: {e}")
        return False

def populate_all_fact_tables(truncate=False):
    """Populate all fact tables in correct dependency order"""
    
    # Processing order matters - no external dependencies for fact_orders
    processing_order = [
        'fact_orders',          # No dependencies
        'fact_line_items'       # Depends on fact_orders
    ]
    
    logger.info("=" * 70)
    logger.info("FACT TABLES POPULATION PROCESS (USING BUSINESS KEYS)")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Database: {DB_NAME} @ {DB_HOST}")
    logger.info(f"Truncate existing: {truncate}")
    logger.info("=" * 70)
    
    conn = get_db_connection()
    results = {}
    
    for fact_table in processing_order:
        results[fact_table] = populate_fact_table(conn, fact_table, truncate=truncate)
    
    conn.close()
    
    # Print summary
    logger.info("\n" + "=" * 70)
    logger.info("POPULATION SUMMARY")
    logger.info("=" * 70)
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for fact_table, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{fact_table:25} {status}")
    
    logger.info("=" * 70)
    logger.info(f"Result: {success_count}/{total_count} fact tables populated successfully")
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
        
        # Display results with formatting
        print(df.to_string(index=False))
        print()
        
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
        'all_table_counts',
        'orphaned_records',
        'fact_data_sample'
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
        orphaned_issues = orphaned_df[orphaned_df['count'] > 0]
        
        if len(orphaned_issues) > 0:
            logger.warning(f"⚠ Found {len(orphaned_issues)} type(s) of orphaned records:")
            for _, row in orphaned_issues.iterrows():
                logger.warning(f"  - {row['check_name']}: {row['count']} records")
            issues_found = True
        else:
            logger.info("✓ No orphaned records found")
    
    # Check table counts
    if results.get('all_table_counts') is not None:
        counts_df = results['all_table_counts']
        fact_tables = counts_df[counts_df['table_name'].str.startswith('fact_')]
        
        logger.info("\nFact Table Metrics:")
        for _, row in fact_tables.iterrows():
            logger.info(f"  {row['table_name']}: {row['row_count']:,} records")
    
    if not issues_found:
        logger.info("✓ All data quality checks passed - no issues found")
    
    logger.info("=" * 70)
    
    return not issues_found

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
    
    # Parse arguments
    truncate = False
    skip_verification = False
    
    if '--truncate' in sys.argv:
        truncate = True
        logger.warning("TRUNCATE mode enabled - existing fact data will be removed")
    
    if '--skip-verification' in sys.argv:
        skip_verification = True
        logger.info("Verification checks will be skipped")
    
    # Populate all fact tables
    success = populate_all_fact_tables(truncate=truncate)
    
    # Run verification checks
    if success and not skip_verification:
        verify_success = run_all_verifications()
        success = success and verify_success
    
    sys.exit(0 if success else 1)