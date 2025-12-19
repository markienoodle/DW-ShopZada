-- 1) Create target deduped table (run once)
CREATE TABLE IF NOT EXISTS staging1_schema.merchant_data_deduped (
    merchant_id     VARCHAR(13),
    creation_date   DATE,
    name            VARCHAR(40),
    street          VARCHAR(40),
    state           VARCHAR(27),
    city            VARCHAR(20),
    country         VARCHAR(52),
    contact_number  VARCHAR(20)
);

-- 2) For each load: clear deduped table
TRUNCATE TABLE staging1_schema.merchant_data_deduped;

-- 3) Deduplicate from cleaned_raw into deduped
WITH ranked AS (
    SELECT
        r.merchant_id,
        r.creation_date,
        r.name,
        r.street,
        r.state,
        r.city,
        r.country,
        r.contact_number,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY r.merchant_id
            ORDER BY r.ingested_at DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY r.merchant_id, r.ingested_at
        ) AS cnt_same_ts
    FROM staging1_schema.merchant_data_cleaned r
),
grouped AS (
    SELECT
        merchant_id,
        ingested_at,
        COUNT(
            DISTINCT (
                creation_date,
                name,
                street,
                state,
                city,
                country,
                contact_number
            )
        ) AS distinct_business
    FROM ranked
    GROUP BY merchant_id, ingested_at
)
INSERT INTO staging1_schema.merchant_data_deduped (
    merchant_id,
    creation_date,
    name,
    street,
    state,
    city,
    country,
    contact_number
)
SELECT
    r.merchant_id,
    r.creation_date,
    r.name,
    r.street,
    r.state,
    r.city,
    r.country,
    r.contact_number
FROM ranked r
JOIN grouped g
  ON r.merchant_id = g.merchant_id
 AND r.ingested_at = g.ingested_at
WHERE
    r.rn = 1
    AND (
        -- only one row at latest ingested_at for this merchant_id
        r.cnt_same_ts = 1
        -- or: multiple rows at latest ingested_at but all business fields identical (true duplicates)
        OR (r.cnt_same_ts > 1 AND g.distinct_business = 1)
    );
