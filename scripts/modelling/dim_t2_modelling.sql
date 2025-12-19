-- =====================================================
-- DIMENSION TABLES POPULATION (USING BUSINESS KEYS)
-- =====================================================

-- -----------------------------------------------------
-- 1. dim_location
-- Combines all unique locations from users, merchants, and staff
-- -----------------------------------------------------
INSERT INTO dim_location (location_sk, street, city, state, country, is_current, valid_from, valid_to)
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY street, city, state, country) as location_sk,
    street,
    city,
    state,
    country,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM (
    -- User locations
    SELECT street, city, state, country
    FROM staging1_schema.user_data_deduped
    
    UNION
    
    -- Merchant locations
    SELECT street, city, state, country
    FROM staging1_schema.merchant_data_deduped
    
    UNION
    
    -- Staff locations
    SELECT street, city, state, country
    FROM staging1_schema.staff_data_deduped
) all_locations
WHERE street IS NOT NULL 
  AND city IS NOT NULL 
  AND state IS NOT NULL 
  AND country IS NOT NULL;

-- -----------------------------------------------------
-- 2. dim_user
-- Combines user_data, user_job, and links to location
-- -----------------------------------------------------
INSERT INTO dim_user (
    user_sk, user_id, name, birthdate, gender, device_address, 
    creation_date, user_type, job_title, job_level, 
    location_sk, is_current, valid_from, valid_to
)
SELECT 
    ud.user_id as user_sk,
    ud.user_id,
    ud.name,
    ud.birthdate::DATE,
    ud.gender,
    ud.device_address,
    ud.creation_date,
    ud.user_type,
    uj.job_title,
    uj.job_level,
    dl.location_sk,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM staging1_schema.user_data_deduped ud
LEFT JOIN staging1_schema.user_job_deduped uj 
    ON ud.user_id = uj.user_id
LEFT JOIN dim_location dl 
    ON ud.street = dl.street 
    AND ud.city = dl.city 
    AND ud.state = dl.state 
    AND ud.country = dl.country
    AND dl.is_current = TRUE;

-- -----------------------------------------------------
-- 3. dim_issuing_bank
-- Links users to their issuing banks
-- -----------------------------------------------------
INSERT INTO dim_issuing_bank (
    bank_sk, user_sk, issuing_bank, is_current, valid_from, valid_to
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY ucc.user_id, ucc.issuing_bank) as bank_sk,
    ucc.user_id as user_sk,
    ucc.issuing_bank,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM staging1_schema.user_credit_card_deduped ucc;

-- -----------------------------------------------------
-- 4. dim_product
-- Combines product list with line item data
-- -----------------------------------------------------
INSERT INTO dim_product (
    product_sk, product_id, product_name, product_type, price, 
    is_current, valid_from, valid_to
)
SELECT DISTINCT
    COALESCE(pl.product_id, li.product_id) as product_sk,
    COALESCE(pl.product_id, li.product_id) as product_id,
    COALESCE(pl.product_name, li.product_name) as product_name,
    pl.product_type,
    COALESCE(pl.price, li.price) as price,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM staging1_schema.product_list_deduped pl
FULL OUTER JOIN staging1_schema.line_item_data_deduped li
    ON pl.product_id = li.product_id
WHERE COALESCE(pl.product_id, li.product_id) IS NOT NULL;

-- -----------------------------------------------------
-- 5. dim_merchant
-- Merchant data with location reference
-- -----------------------------------------------------
INSERT INTO dim_merchant (
    merchant_sk, merchant_id, name, contact_number, creation_date, 
    location_sk, is_current, valid_from, valid_to
)
SELECT 
    md.merchant_id as merchant_sk,
    md.merchant_id,
    md.name,
    md.contact_number,
    md.creation_date,
    dl.location_sk,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM staging1_schema.merchant_data_deduped md
LEFT JOIN dim_location dl 
    ON md.street = dl.street 
    AND md.city = dl.city 
    AND md.state = dl.state 
    AND md.country = dl.country
    AND dl.is_current = TRUE;

-- -----------------------------------------------------
-- 6. dim_staff
-- Staff data with location reference
-- -----------------------------------------------------
INSERT INTO dim_staff (
    staff_sk, staff_id, name, job_level, contact_number, 
    creation_date, location_sk, is_current, valid_from, valid_to
)
SELECT 
    sd.staff_id as staff_sk,
    sd.staff_id,
    sd.name,
    sd.job_level,
    sd.contact_number,
    sd.creation_date,
    dl.location_sk,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM staging1_schema.staff_data_deduped sd
LEFT JOIN dim_location dl 
    ON sd.street = dl.street 
    AND sd.city = dl.city 
    AND sd.state = dl.state 
    AND sd.country = dl.country
    AND dl.is_current = TRUE;

-- -----------------------------------------------------
-- 7. dim_campaign
-- Campaign master data
-- -----------------------------------------------------
INSERT INTO dim_campaign (
    campaign_sk, campaign_id, campaign_name, campaign_description, 
    discount, is_current, valid_from, valid_to
)
SELECT 
    campaign_id as campaign_sk,
    campaign_id,
    campaign_name,
    campaign_description,
    discount,
    TRUE as is_current,
    CURRENT_DATE as valid_from,
    NULL as valid_to
FROM staging1_schema.campaign_data_deduped;