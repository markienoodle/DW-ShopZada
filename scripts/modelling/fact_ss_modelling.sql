-- =====================================================
-- SCD TYPE 2 TRANSFORMATION: T2 -> STAR SCHEMA
-- FACT TABLES
-- =====================================================

-- -----------------------------------------------------
-- 1. fact_orders - Load with Current Dimension SKs
-- -----------------------------------------------------

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
);

-- -----------------------------------------------------
-- 2. fact_line_items - Load with Current Dimension SKs
-- -----------------------------------------------------

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
);

-- -----------------------------------------------------
-- VERIFICATION QUERIES
-- -----------------------------------------------------

-- Check dimension record counts by currency status
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
FROM dim_campaign;

-- Check fact table counts
SELECT 'fact_orders' as table_name, COUNT(*) as row_count 
FROM fact_orders
UNION ALL
SELECT 'fact_line_items', COUNT(*) 
FROM fact_line_items;

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
    'Line items with NULL product_sk',
    COUNT(*)
FROM fact_line_items
WHERE product_sk IS NULL;

-- Check for multiple current records (data quality issue)
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
) dup;