-- 1) Create target deduped table (run once)
CREATE TABLE IF NOT EXISTS staging1_schema.user_data_deduped (
    user_id        VARCHAR(9),
    creation_date  DATE,
    name           VARCHAR(40),
    street         VARCHAR(40),
    state          VARCHAR(27),
    city           VARCHAR(20),
    country        VARCHAR(52),
    birthdate      TIMESTAMP,
    gender         VARCHAR(6),
    device_address VARCHAR(17),
    user_type      VARCHAR(8)
);

-- 2) For each load: clear deduped table
TRUNCATE TABLE staging1_schema.user_data_deduped;

-- 3) Deduplicate from user_data_cleaned into user_data_deduped
WITH ranked AS (
    SELECT
        r.user_id,
        r.creation_date,
        r.name,
        r.street,
        r.state,
        r.city,
        r.country,
        r.birthdate,
        r.gender,
        r.device_address,
        r.user_type,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.user_id,
                r.creation_date,
                r.name,
                r.street,
                r.state,
                r.city,
                r.country,
                r.birthdate,
                r.gender,
                r.device_address,
                r.user_type
            ORDER BY r.ingested_at DESC
        ) AS rn
    FROM staging1_schema.user_data_cleaned r
)
INSERT INTO staging1_schema.user_data_deduped (
    user_id,
    creation_date,
    name,
    street,
    state,
    city,
    country,
    birthdate,
    gender,
    device_address,
    user_type
)
SELECT
    user_id,
    creation_date,
    name,
    street,
    state,
    city,
    country,
    birthdate,
    gender,
    device_address,
    user_type
FROM ranked
WHERE rn = 1;
