use gmall_log;


-- 用户维度表
DROP TABLE IF EXISTS gmall_log.dim_user;
CREATE EXTERNAL TABLE gmall_log.dim_user (
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '性别',
    age INT COMMENT '年龄',
    user_level STRING COMMENT '用户等级',
    register_time STRING COMMENT '注册时间',
    source_type STRING COMMENT '用户来源'
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 商品维度表
DROP TABLE IF EXISTS gmall_log.dim_product;
CREATE EXTERNAL TABLE gmall_log.dim_product (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    brand_id STRING COMMENT '品牌ID',
    category1_id STRING COMMENT '一级类目ID',
    category2_id STRING COMMENT '二级类目ID',
    category3_id STRING COMMENT '三级类目ID',
    price DOUBLE COMMENT '价格',
    onsale_flag STRING COMMENT '是否在售'
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 品牌维度表
DROP TABLE IF EXISTS gmall_log.dim_brand;
CREATE EXTERNAL TABLE gmall_log.dim_brand (
    brand_id STRING COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称'
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 类目维度表
DROP TABLE IF EXISTS gmall_log.dim_category;
CREATE EXTERNAL TABLE gmall_log.dim_category (
    category1_id STRING COMMENT '一级类目ID',
    category1_name STRING COMMENT '一级类目名称',
    category2_id STRING COMMENT '二级类目ID',
    category2_name STRING COMMENT '二级类目名称',
    category3_id STRING COMMENT '三级类目ID',
    category3_name STRING COMMENT '三级类目名称'
)
PARTITIONED BY (ds STRING)
STORED AS ORC;

-- 日期维度表
DROP TABLE IF EXISTS gmall_log.dim_date;
CREATE EXTERNAL TABLE gmall_log.dim_date (
    date_id STRING COMMENT '日期ID',
    date_day STRING COMMENT '日期',
    day_of_week INT COMMENT '星期几',
    day_name STRING COMMENT '星期名称',
    is_weekend STRING COMMENT '是否周末',
    week_of_year INT COMMENT '一年第几周',
    month INT COMMENT '月份',
    month_name STRING COMMENT '月份名称',
    quarter INT COMMENT '季度',
    year INT COMMENT '年份'
)
PARTITIONED BY (ds STRING)
STORED AS ORC;
