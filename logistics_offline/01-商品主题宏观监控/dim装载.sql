USE gmall_log;

-- 装载 dim_user
INSERT OVERWRITE TABLE dim_user PARTITION (ds='20250808')
SELECT
    user_id,
    user_name,
    gender,
    CAST(age AS INT),
    user_level,
    register_time,
    source_type
FROM ods_user_info_full
WHERE ds = '20250808';

-- 装载 dim_product
INSERT OVERWRITE TABLE dim_product PARTITION (ds='20250808')
SELECT
    product_id,
    product_name,
    brand_id,
    category1_id,
    category2_id,
    category3_id,
    CAST(price AS DOUBLE) AS price,
    is_on_sale
FROM ods_product_info_full
WHERE ds = '20250808';


-- 装载 dim_brand
INSERT OVERWRITE TABLE dim_brand PARTITION (ds='20250808')
SELECT DISTINCT
    brand_id,
    brand_name
FROM ods_product_info_full
WHERE ds = '20250808';

-- 装载 dim_category
USE gmall_log;

INSERT OVERWRITE TABLE dim_category PARTITION (ds = '20250808')
SELECT
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name
FROM ods_product_info_full
WHERE ds = '20250808';


INSERT OVERWRITE TABLE dim_date PARTITION (ds = '99999999')
SELECT
    date_format(date_add('2015-01-01', n), 'yyyyMMdd') AS date_id,
    date_format(date_add('2015-01-01', n), 'yyyy-MM-dd') AS date_day,
    dayofweek(date_add('2015-01-01', n)) AS day_of_week,
    CASE dayofweek(date_add('2015-01-01', n))
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name,
    CASE WHEN dayofweek(date_add('2015-01-01', n)) IN (1, 7) THEN 'Y' ELSE 'N' END AS is_weekend,
    weekofyear(date_add('2015-01-01', n)) AS week_of_year,
    month(date_add('2015-01-01', n)) AS month,
    CASE month(date_add('2015-01-01', n))
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    quarter(date_add('2015-01-01', n)) AS quarter,
    year(date_add('2015-01-01', n)) AS year
FROM (
    SELECT
        a.pos + b.pos * 100 + c.pos * 10000 AS n
    FROM
        (SELECT posexplode(split(space(100), '')) AS (pos, val)) a
    CROSS JOIN
        (SELECT posexplode(split(space(100), '')) AS (pos, val)) b
    CROSS JOIN
        (SELECT posexplode(split(space(1), '')) AS (pos, val)) c
) tmp
WHERE n < 3653;