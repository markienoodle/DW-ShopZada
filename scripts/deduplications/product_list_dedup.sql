-- 1) Create target staging1 table (run once)
CREATE SCHEMA IF NOT EXISTS staging1_schema;

CREATE TABLE IF NOT EXISTS staging1_schema.product_list_deduped (
    product_id    VARCHAR(20),
    product_name  VARCHAR(255),
    product_type  VARCHAR(255),
    price         NUMERIC(10,2)
);

-- 2) For each load: clear staging1 table
TRUNCATE TABLE staging1_schema.product_list_deduped;

-- 3) Deduplicate from staging1 into staging1
WITH ranked AS (
    SELECT
        r.product_id,
        r.product_name,
        r.product_type,
        r.price,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY r.product_id
            ORDER BY r.ingested_at DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY r.product_id, r.ingested_at
        ) AS cnt_same_ts
    FROM staging1_schema.product_list_cleaned r
),
grouped AS (
    SELECT
        product_id,
        ingested_at,
        COUNT(
            DISTINCT (product_name, product_type, price)
        ) AS distinct_business
    FROM ranked
    GROUP BY product_id, ingested_at
)
INSERT INTO staging1_schema.product_list_deduped (
    product_id,
    product_name,
    product_type,
    price
)
SELECT
    r.product_id,
    r.product_name,
    r.product_type,
    r.price
FROM ranked r
JOIN grouped g
  ON r.product_id = g.product_id
 AND r.ingested_at = g.ingested_at
WHERE
    r.rn = 1
    AND (
        -- latest timestamp is unique for this id
        r.cnt_same_ts = 1
        -- OR: multiple rows at latest ts but all business fields identical (true duplicates)
        OR (r.cnt_same_ts > 1 AND g.distinct_business = 1)
    );
