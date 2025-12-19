-- 1) Create target staging1 table (run once)

CREATE TABLE IF NOT EXISTS staging1_schema.campaign_data_deduped (
    campaign_id          VARCHAR(13),
    campaign_name        VARCHAR(60),
    campaign_description VARCHAR(150),
    discount             NUMERIC(5, 4)
);

-- 2) For each load: clear staging1 table
TRUNCATE TABLE staging1_schema.campaign_data_deduped;

-- 3) Deduplicate from staging1 into staging1
WITH ranked AS (
    SELECT
        r.campaign_id,
        r.campaign_name,
        r.campaign_description,
        r.discount,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY r.campaign_id
            ORDER BY r.ingested_at DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY r.campaign_id, r.ingested_at
        ) AS cnt_same_ts
    FROM staging1_schema.campaign_data_cleaned r
),
grouped AS (
    SELECT
        campaign_id,
        ingested_at,
        COUNT(
            DISTINCT (campaign_name, campaign_description, discount)
        ) AS distinct_business
    FROM ranked
    GROUP BY campaign_id, ingested_at
)
INSERT INTO staging1_schema.campaign_data_deduped (
    campaign_id,
    campaign_name,
    campaign_description,
    discount
)
SELECT
    r.campaign_id,
    r.campaign_name,
    r.campaign_description,
    r.discount
FROM ranked r
JOIN grouped g
  ON r.campaign_id = g.campaign_id
 AND r.ingested_at = g.ingested_at
WHERE
    r.rn = 1
    AND (
        -- latest timestamp is unique for this id
        r.cnt_same_ts = 1
        -- OR: multiple rows at latest ts but all business fields identical
        OR (r.cnt_same_ts > 1 AND g.distinct_business = 1)
    );
