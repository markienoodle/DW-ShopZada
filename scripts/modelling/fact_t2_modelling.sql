-- =====================================================
-- FACT TABLES POPULATION (USING BUSINESS KEYS)
-- =====================================================

-- -----------------------------------------------------
-- 1. fact_orders
-- Combines order data with transactional campaign data
-- -----------------------------------------------------
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
    ON od.order_id = tcd.order_id;

-- -----------------------------------------------------
-- 2. fact_line_items
-- Line items linked to orders, products, and dimensions
-- -----------------------------------------------------
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
    ON li.order_id = fo.order_id;

-- -----------------------------------------------------
-- VERIFICATION QUERIES
-- -----------------------------------------------------

-- Check row counts
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
SELECT 'fact_line_items', COUNT(*) FROM fact_line_items;

-- Check for orphaned records in fact tables
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
WHERE product_sk IS NULL;