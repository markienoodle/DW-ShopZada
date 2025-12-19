-- 1) Create target staging2 table (run once)
CREATE SCHEMA IF NOT EXISTS staging2_schema;

CREATE TABLE IF NOT EXISTS staging2_schema.line_item_data_stg2 (
    order_id     VARCHAR(36),
    price        DECIMAL(10,2),
    quantity     INTEGER,
    product_name VARCHAR(100),
    product_id   VARCHAR(12)
);

-- 2) For each load: clear staging2 table
TRUNCATE TABLE staging2_schema.line_item_data_stg2;

-- 3) Deduplicate from staging1 into staging2
WITH ranked AS (
    SELECT
        r.order_id,
        r.price,
        r.quantity,
        r.product_name,
        r.product_id,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.order_id,
                r.product_id,
                r.product_name,
                r.price,
                r.quantity
            ORDER BY r.ingested_at DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY
                r.order_id,
                r.product_id,
                r.product_name,
                r.price,
                r.quantity,
                r.ingested_at
        ) AS cnt_same_ts
    FROM staging1_schema.line_item_data_cleaned_raw r
),
grouped AS (
    SELECT
        order_id,
        product_id,
        product_name,
        price,
        quantity,
        ingested_at,
        COUNT(*) AS cnt_rows
    FROM ranked
    GROUP BY
        order_id,
        product_id,
        product_name,
        price,
        quantity,
        ingested_at
)
INSERT INTO staging2_schema.line_item_data_stg2 (
    order_id,
    price,
    quantity,
    product_name,
    product_id
)
SELECT
    r.order_id,
    r.price,
    r.quantity,
    r.product_name,
    r.product_id
FROM ranked r
JOIN grouped g
  ON r.order_id     = g.order_id
 AND r.product_id   = g.product_id
 AND r.product_name = g.product_name
 AND r.price        = g.price
 AND r.quantity     = g.quantity
 AND r.ingested_at  = g.ingested_at
WHERE
    r.rn = 1
    AND (
        -- latest ingested_at for this exact business row
        r.cnt_same_ts = 1
        -- or: multiple exact duplicates at same ts -> keep one, drop others
        OR (r.cnt_same_ts > 1 AND g.cnt_rows = r.cnt_same_ts)
    );
