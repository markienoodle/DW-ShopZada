-- 1) Create target deduped table (run once)
CREATE TABLE IF NOT EXISTS staging1_schema.user_job_deduped (
    user_id    VARCHAR(9),
    name       VARCHAR(40),
    job_title  VARCHAR(20),
    job_level  VARCHAR(20)
);

-- 2) For each load: clear deduped table
TRUNCATE TABLE staging1_schema.user_job_deduped;

-- 3) Deduplicate from user_job_cleaned into user_job_deduped
WITH ranked AS (
    SELECT
        r.user_id,
        r.name,
        r.job_title,
        r.job_level,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.user_id,
                r.name,
                r.job_title,
                r.job_level
            ORDER BY r.ingested_at DESC
        ) AS rn
    FROM staging1_schema.user_job_cleaned r
)
INSERT INTO staging1_schema.user_job_deduped (
    user_id,
    name,
    job_title,
    job_level
)
SELECT
    user_id,
    name,
    job_title,
    job_level
FROM ranked
WHERE rn = 1;
