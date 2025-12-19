-- 1) Create target deduped table (run once)
CREATE TABLE IF NOT EXISTS staging1_schema.staff_data_deduped (
    staff_id        VARCHAR(12),
    name            VARCHAR(40),
    job_level       VARCHAR(20),
    street          VARCHAR(40),
    state           VARCHAR(27),
    city            VARCHAR(20),
    country         VARCHAR(52),
    contact_number  VARCHAR(20),
    creation_date   DATE
);

-- 2) For each load: clear deduped table
TRUNCATE TABLE staging1_schema.staff_data_deduped;

-- 3) Deduplicate from cleaned_raw into deduped
WITH ranked AS (
    SELECT
        r.staff_id,
        r.name,
        r.job_level,
        r.street,
        r.state,
        r.city,
        r.country,
        r.contact_number,
        r.creation_date,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.staff_id,
                r.name,
                r.job_level,
                r.street,
                r.state,
                r.city,
                r.country,
                r.contact_number,
                r.creation_date
            ORDER BY r.ingested_at DESC
        ) AS rn
    FROM staging1_schema.staff_data_cleaned r
)
INSERT INTO staging1_schema.staff_data_deduped (
    staff_id,
    name,
    job_level,
    street,
    state,
    city,
    country,
    contact_number,
    creation_date
)
SELECT
    staff_id,
    name,
    job_level,
    street,
    state,
    city,
    country,
    contact_number,
    creation_date
FROM ranked
WHERE rn = 1;
