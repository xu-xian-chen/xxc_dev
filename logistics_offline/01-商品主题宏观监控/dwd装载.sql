USE gmall_log;

-- 1. dwd_order_info
INSERT OVERWRITE TABLE dwd_order_info PARTITION (ds = '20250808')
SELECT
    order_id,
    user_id,
    order_status,
    order_time,
    total_amount,
    pay_time,
    pay_type,
    is_presell
FROM ods_order_info_inc
WHERE ds = '20250808';

-- 2. dwd_order_detail
INSERT OVERWRITE TABLE dwd_order_detail PARTITION (ds = '20250808')
SELECT
    detail_id,
    order_id,
    product_id,
    product_name,
    price,
    quantity,
    sku_spec
FROM ods_order_detail_inc
WHERE ds = '20250808';

-- 3. dwd_user_behavior_log
INSERT OVERWRITE TABLE dwd_user_behavior_log PARTITION (ds = '20250808')
SELECT
    log_id,
    user_id,
    product_id,
    behavior_type,
    device_type,
    channel,
    stay_time,
    is_jump,
    event_time,
    session_id
FROM ods_user_behavior_log_inc
WHERE ds = '20250808';

-- 4. dwd_refund_info
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwd_refund_info PARTITION (ds = '20250808')
SELECT
    refund_id,
    order_id,
    product_id,
    refund_type, -- ✅ 这一列你刚刚漏掉了
    refund_amount,
    is_success AS refund_status,
    refund_time
FROM ods_refund_info_inc
WHERE ds = '20250808';


-- 5. dwd_user_info
INSERT OVERWRITE TABLE dwd_user_info PARTITION (ds = '20250808')
SELECT
    user_id,
    user_name,
    gender,
    age,
    user_level,
    register_time,
    source_type
FROM ods_user_info_full
WHERE ds = '20250808';

-- 6. dwd_product_info
INSERT OVERWRITE TABLE dwd_product_info PARTITION (ds = '20250808')
SELECT
    product_id,
    product_name,
    brand_id,
    brand_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    price,
    is_on_sale,
    create_time
FROM ods_product_info_full
WHERE ds = '20250808';
