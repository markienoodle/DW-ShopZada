-- =====================================================
-- SCD TYPE 2 TRANSFORMATION: T2 -> STAR SCHEMA
-- DIMENSION TABLES
-- =====================================================

-- -----------------------------------------------------
-- 1. dim_location - SCD Type 2 Processing
-- -----------------------------------------------------

-- Step 1: Identify changed records
WITH location_changes AS (
    SELECT 
        t2.location_sk,
        t2.street,
        t2.city,
        t2.state,
        t2.country,
        dim.location_sk as existing_sk,
        CASE 
            WHEN dim.location_sk IS NULL THEN 'INSERT'
            WHEN (dim.street != t2.street OR 
                  dim.city != t2.city OR 
                  dim.state != t2.state OR 
                  dim.country != t2.country) AND dim.is_current = TRUE 
            THEN 'UPDATE'
            ELSE 'NO_CHANGE'
        END as change_type
    FROM t2_schema.dim_location t2
    LEFT JOIN dim_location dim 
        ON t2.location_sk = dim.location_sk
        AND dim.is_current = TRUE
)
-- Step 2: Expire old records
UPDATE dim_location
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE location_sk IN (
    SELECT existing_sk 
    FROM location_changes 
    WHERE change_type = 'UPDATE'
)
AND is_current = TRUE;

-- Step 3: Insert new and changed records
INSERT INTO dim_location (street, city, state, country, is_current, valid_from, valid_to)
SELECT 
    street,
    city,
    state,
    country,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_location t2
WHERE location_sk IN (
    SELECT location_sk 
    FROM (
        SELECT 
            t2.location_sk,
            CASE 
                WHEN dim.location_sk IS NULL THEN 'INSERT'
                WHEN (dim.street != t2.street OR 
                      dim.city != t2.city OR 
                      dim.state != t2.state OR 
                      dim.country != t2.country) AND dim.is_current = TRUE 
                THEN 'UPDATE'
                ELSE 'NO_CHANGE'
            END as change_type
        FROM t2_schema.dim_location t2
        LEFT JOIN dim_location dim 
            ON t2.location_sk = dim.location_sk
            AND dim.is_current = TRUE
    ) changes
    WHERE change_type IN ('INSERT', 'UPDATE')
);

-- -----------------------------------------------------
-- 2. dim_user - SCD Type 2 Processing
-- -----------------------------------------------------

-- Expire changed records
UPDATE dim_user
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE user_id IN (
    SELECT t2.user_id
    FROM t2_schema.dim_user t2
    INNER JOIN dim_user dim 
        ON t2.user_id = dim.user_id
        AND dim.is_current = TRUE
    WHERE t2.name != dim.name OR
          t2.birthdate != dim.birthdate OR
          COALESCE(t2.gender, '') != COALESCE(dim.gender, '') OR
          COALESCE(t2.device_address, '') != COALESCE(dim.device_address, '') OR
          COALESCE(t2.user_type, '') != COALESCE(dim.user_type, '') OR
          COALESCE(t2.job_title, '') != COALESCE(dim.job_title, '') OR
          COALESCE(t2.job_level, '') != COALESCE(dim.job_level, '') OR
          COALESCE(t2.location_sk, 0) != COALESCE(dim.location_sk, 0)
)
AND is_current = TRUE;

-- Insert new and changed records
INSERT INTO dim_user (
    user_id, name, birthdate, gender, device_address, 
    creation_date, user_type, job_title, job_level, 
    location_sk, is_current, valid_from, valid_to
)
SELECT 
    t2.user_id,
    t2.name,
    t2.birthdate,
    t2.gender,
    t2.device_address,
    t2.creation_date,
    t2.user_type,
    t2.job_title,
    t2.job_level,
    loc_new.location_sk,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_user t2
LEFT JOIN dim_location loc_new 
    ON t2.location_sk = loc_new.location_sk
    AND loc_new.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_user dim 
    WHERE dim.user_id = t2.user_id 
    AND dim.is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM dim_user dim
    WHERE dim.user_id = t2.user_id
    AND dim.is_current = FALSE
    AND dim.valid_to = CURRENT_DATE
);

-- -----------------------------------------------------
-- 3. dim_issuing_bank - SCD Type 2 Processing
-- -----------------------------------------------------

-- Expire changed records
UPDATE dim_issuing_bank
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE bank_sk IN (
    SELECT dim.bank_sk
    FROM t2_schema.dim_issuing_bank t2
    INNER JOIN dim_issuing_bank dim 
        ON t2.bank_sk = dim.bank_sk
        AND dim.is_current = TRUE
    WHERE t2.issuing_bank != dim.issuing_bank OR
          t2.user_sk != dim.user_sk
)
AND is_current = TRUE;

-- Insert new and changed records
INSERT INTO dim_issuing_bank (user_sk, issuing_bank, is_current, valid_from, valid_to)
SELECT 
    u.user_sk,
    t2.issuing_bank,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_issuing_bank t2
INNER JOIN dim_user u 
    ON t2.user_sk = u.user_id
    AND u.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_issuing_bank dim 
    WHERE dim.user_sk = u.user_sk 
    AND dim.issuing_bank = t2.issuing_bank
    AND dim.is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM dim_issuing_bank dim
    WHERE dim.bank_sk = t2.bank_sk
    AND dim.is_current = FALSE
    AND dim.valid_to = CURRENT_DATE
);

-- -----------------------------------------------------
-- 4. dim_product - SCD Type 2 Processing
-- -----------------------------------------------------

-- Expire changed records
UPDATE dim_product
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE product_id IN (
    SELECT t2.product_id
    FROM t2_schema.dim_product t2
    INNER JOIN dim_product dim 
        ON t2.product_id = dim.product_id
        AND dim.is_current = TRUE
    WHERE t2.product_name != dim.product_name OR
          COALESCE(t2.product_type, '') != COALESCE(dim.product_type, '') OR
          t2.price != dim.price
)
AND is_current = TRUE;

-- Insert new and changed records
INSERT INTO dim_product (
    product_id, product_name, product_type, price, 
    is_current, valid_from, valid_to
)
SELECT 
    t2.product_id,
    t2.product_name,
    t2.product_type,
    t2.price,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_product t2
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_product dim 
    WHERE dim.product_id = t2.product_id 
    AND dim.is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM dim_product dim
    WHERE dim.product_id = t2.product_id
    AND dim.is_current = FALSE
    AND dim.valid_to = CURRENT_DATE
);

-- -----------------------------------------------------
-- 5. dim_merchant - SCD Type 2 Processing
-- -----------------------------------------------------

-- Expire changed records
UPDATE dim_merchant
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE merchant_id IN (
    SELECT t2.merchant_id
    FROM t2_schema.dim_merchant t2
    INNER JOIN dim_merchant dim 
        ON t2.merchant_id = dim.merchant_id
        AND dim.is_current = TRUE
    WHERE t2.name != dim.name OR
          COALESCE(t2.contact_number, '') != COALESCE(dim.contact_number, '') OR
          COALESCE(t2.location_sk, 0) != COALESCE(dim.location_sk, 0)
)
AND is_current = TRUE;

-- Insert new and changed records
INSERT INTO dim_merchant (
    merchant_id, name, contact_number, creation_date, 
    location_sk, is_current, valid_from, valid_to
)
SELECT 
    t2.merchant_id,
    t2.name,
    t2.contact_number,
    t2.creation_date,
    loc_new.location_sk,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_merchant t2
LEFT JOIN dim_location loc_new 
    ON t2.location_sk = loc_new.location_sk
    AND loc_new.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_merchant dim 
    WHERE dim.merchant_id = t2.merchant_id 
    AND dim.is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM dim_merchant dim
    WHERE dim.merchant_id = t2.merchant_id
    AND dim.is_current = FALSE
    AND dim.valid_to = CURRENT_DATE
);

-- -----------------------------------------------------
-- 6. dim_staff - SCD Type 2 Processing
-- -----------------------------------------------------

-- Expire changed records
UPDATE dim_staff
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE staff_id IN (
    SELECT t2.staff_id
    FROM t2_schema.dim_staff t2
    INNER JOIN dim_staff dim 
        ON t2.staff_id = dim.staff_id
        AND dim.is_current = TRUE
    WHERE t2.name != dim.name OR
          COALESCE(t2.job_level, '') != COALESCE(dim.job_level, '') OR
          COALESCE(t2.contact_number, '') != COALESCE(dim.contact_number, '') OR
          COALESCE(t2.location_sk, 0) != COALESCE(dim.location_sk, 0)
)
AND is_current = TRUE;

-- Insert new and changed records
INSERT INTO dim_staff (
    staff_id, name, job_level, contact_number, 
    creation_date, location_sk, is_current, valid_from, valid_to
)
SELECT 
    t2.staff_id,
    t2.name,
    t2.job_level,
    t2.contact_number,
    t2.creation_date,
    loc_new.location_sk,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_staff t2
LEFT JOIN dim_location loc_new 
    ON t2.location_sk = loc_new.location_sk
    AND loc_new.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_staff dim 
    WHERE dim.staff_id = t2.staff_id 
    AND dim.is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM dim_staff dim
    WHERE dim.staff_id = t2.staff_id
    AND dim.is_current = FALSE
    AND dim.valid_to = CURRENT_DATE
);

-- -----------------------------------------------------
-- 7. dim_campaign - SCD Type 2 Processing
-- -----------------------------------------------------

-- Expire changed records
UPDATE dim_campaign
SET is_current = FALSE,
    valid_to = CURRENT_DATE
WHERE campaign_id IN (
    SELECT t2.campaign_id
    FROM t2_schema.dim_campaign t2
    INNER JOIN dim_campaign dim 
        ON t2.campaign_id = dim.campaign_id
        AND dim.is_current = TRUE
    WHERE t2.campaign_name != dim.campaign_name OR
          COALESCE(t2.campaign_description, '') != COALESCE(dim.campaign_description, '') OR
          t2.discount != dim.discount
)
AND is_current = TRUE;

-- Insert new and changed records
INSERT INTO dim_campaign (
    campaign_id, campaign_name, campaign_description, 
    discount, is_current, valid_from, valid_to
)
SELECT 
    t2.campaign_id,
    t2.campaign_name,
    t2.campaign_description,
    t2.discount,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM t2_schema.dim_campaign t2
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_campaign dim 
    WHERE dim.campaign_id = t2.campaign_id 
    AND dim.is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM dim_campaign dim
    WHERE dim.campaign_id = t2.campaign_id
    AND dim.is_current = FALSE
    AND dim.valid_to = CURRENT_DATE
);