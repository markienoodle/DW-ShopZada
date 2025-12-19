-- 1) Create target staging1 table (run once)
CREATE SCHEMA IF NOT EXISTS staging1_schema;

CREATE TABLE IF NOT EXISTS staging1_schema.order_data_deduped (
    order_id          VARCHAR(36),
    campaign_id       VARCHAR(13),
    user_id           VARCHAR(9),
    merchant_id       VARCHAR(13),
    staff_id          VARCHAR(12),
    transaction_date  DATE,
    estimated_arrival INTEGER,
    delay_in_days     INTEGER,
    availed           BOOLEAN
);

-- 2) For each load: clear staging1 table
TRUNCATE TABLE staging1_schema.order_data_deduped;

-- 3) Deduplicate from staging1 into staging1
WITH ranked AS (
    SELECT
        r.order_id,
        r.campaign_id,
        r.user_id,
        r.merchant_id,
        r.staff_id,
        r.transaction_date,
        r.estimated_arrival,
        r.delay_in_days,
        r.availed,
        r.ingested_at,
        -- latest record per order_id
        ROW_NUMBER() OVER (
            PARTITION BY r.order_id
            ORDER BY r.ingested_at DESC
        ) AS rn,
        -- how many rows share this order_id and this ingested_at
        COUNT(*) OVER (
            PARTITION BY r.order_id, r.ingested_at
        ) AS cnt_same_ts
    FROM staging1_schema.order_data_cleaned r
),
grouped AS (
    -- at each (order_id, latest ingested_at), check if business fields conflict
    SELECT
        order_id,
        ingested_at,
        COUNT(
            DISTINCT (
                campaign_id,
                user_id,
                merchant_id,
                staff_id,
                transaction_date,
                estimated_arrival,
                delay_in_days,
                availed
            )
        ) AS distinct_business
    FROM ranked
    GROUP BY order_id, ingested_at
)
INSERT INTO staging1_schema.order_data_deduped (
    order_id,
    campaign_id,
    user_id,
    merchant_id,
    staff_id,
    transaction_date,
    estimated_arrival,
    delay_in_days,
    availed
)
SELECT
    r.order_id,
    r.campaign_id,
    r.user_id,
    r.merchant_id,
    r.staff_id,
    r.transaction_date,
    r.estimated_arrival,
    r.delay_in_days,
    r.availed
FROM ranked r
JOIN grouped g
  ON r.order_id    = g.order_id
 AND r.ingested_at = g.ingested_at
WHERE
    r.rn = 1
    AND (
        -- case 1: only one row at latest ingested_at for this order_id
        r.cnt_same_ts = 1
        -- case 2: multiple rows at latest ingested_at, but all business fields identical
        OR (r.cnt_same_ts > 1 AND g.distinct_business = 1)
    );
