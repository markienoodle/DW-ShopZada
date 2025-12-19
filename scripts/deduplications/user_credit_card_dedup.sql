-- 1) Create target deduped table (run once)
CREATE TABLE IF NOT EXISTS staging1_schema.user_credit_card_deduped (
    user_id       VARCHAR(9),      -- adjust length if needed
    name          VARCHAR(40),     -- adjust length if needed
    issuing_bank  VARCHAR(20)      -- adjust length if needed
);

-- 2) For each load: clear deduped table
TRUNCATE TABLE staging1_schema.user_credit_card_deduped;

-- 3) Deduplicate from cleaned_raw into deduped
WITH ranked AS (
    SELECT
        r.user_id,
        r.name,
        r.issuing_bank,
        r.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.user_id,
                r.name,
                r.issuing_bank
            ORDER BY r.ingested_at DESC
        ) AS rn
    FROM staging1_schema.user_credit_card_cleaned r
)
INSERT INTO staging1_schema.user_credit_card_deduped (
    user_id,
    name,
    issuing_bank
)
SELECT
    user_id,
    name,
    issuing_bank
FROM ranked
WHERE rn = 1;
