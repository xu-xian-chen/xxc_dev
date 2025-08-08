USE gmall_log;

-- 1. dwd_order_info
DROP TABLE IF EXISTS dwd_order_info;
CREATE EXTERNAL TABLE dwd_order_info (
    order_id STRING,
    user_id STRING,
    order_status STRING,
    order_time STRING,
    total_amount DECIMAL(10, 2),
    pay_time STRING,
    pay_type STRING,
    is_presell BOOLEAN
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 2. dwd_order_detail
DROP TABLE IF EXISTS dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail (
    detail_id STRING,
    order_id STRING,
    product_id STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    quantity INT,
    sku_spec STRING
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 3. dwd_user_behavior_log
DROP TABLE IF EXISTS dwd_user_behavior_log;
CREATE EXTERNAL TABLE dwd_user_behavior_log (
    log_id STRING,
    user_id STRING,
    product_id STRING,
    behavior_type STRING,
    device_type STRING,
    channel STRING,
    stay_time INT,
    is_jump BOOLEAN,
    event_time STRING,
    session_id STRING
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 4. dwd_refund_info
DROP TABLE IF EXISTS gmall_log.dwd_refund_info;

CREATE EXTERNAL TABLE gmall_log.dwd_refund_info (
    refund_id      STRING COMMENT '退款ID',
    order_id       STRING COMMENT '订单ID',
    product_id     STRING COMMENT '商品ID',
    refund_type    STRING COMMENT '退款类型',
    refund_amount  DECIMAL(10, 2) COMMENT '退款金额',
    refund_status  STRING COMMENT '退款状态',
    refund_time    STRING COMMENT '退款时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC;

-- 5. dwd_user_info
DROP TABLE IF EXISTS dwd_user_info;
CREATE EXTERNAL TABLE dwd_user_info (
    user_id STRING,
    user_name STRING,
    gender STRING,
    age INT,
    user_level STRING,
    register_time STRING,
    source_type STRING
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 6. dwd_product_info
DROP TABLE IF EXISTS dwd_product_info;
CREATE EXTERNAL TABLE dwd_product_info (
    product_id STRING,
    product_name STRING,
    brand_id STRING,
    brand_name STRING,
    category1_id STRING,
    category1_name STRING,
    category2_id STRING,
    category2_name STRING,
    category3_id STRING,
    category3_name STRING,
    price DECIMAL(10, 2),
    is_on_sale BOOLEAN,
    create_time STRING
)
PARTITIONED BY (ds STRING)
STORED AS ORC;
