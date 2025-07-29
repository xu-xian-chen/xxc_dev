from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083")\
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
date = "20250707"

spark.sql("use gmall;")
#INSERT OVERWRITE TABLE dim_sku_full PARTITION (dt = {date})
spark.sql(f"""
INSERT OVERWRITE TABLE dim_sku_full PARTITION (dt = {date})
SELECT
  sku.id,
  sku.price,
  sku.sku_name,
  sku.sku_desc,
  sku.weight,
  CAST(sku.is_sale AS BOOLEAN) AS is_sale,
  sku.spu_id,
  spu.spu_name,
  sku.category3_id,
  c3.name,
  c2.id,
  c2.name,
  c1.id,
  c1.name,
  sku.tm_id,
  tm.tm_name,
  attr.attrs,
  sale_attr.sale_attrs,
  sku.create_time
FROM ods_sku_info sku
LEFT JOIN ods_spu_info spu ON sku.spu_id = spu.id AND spu.ds = {date}
LEFT JOIN ods_base_category3 c3 ON sku.category3_id = c3.id AND c3.ds = {date}
LEFT JOIN ods_base_category2 c2 ON c3.category2_id = c2.id AND c2.ds = {date}
LEFT JOIN ods_base_category1 c1 ON c2.category1_id = c1.id AND c1.ds = {date}
LEFT JOIN ods_base_trademark tm ON sku.tm_id = tm.id AND tm.ds = {date}
LEFT JOIN (
  SELECT
    sku_id,
    collect_set(named_struct(
      'attr_id', cast(attr_id as STRING),
      'value_id', cast(value_id as STRING),
      'attr_name', attr_name,
      'value_name', value_name
    )) AS attrs
  FROM ods_sku_attr_value
  WHERE ds = {date}
  GROUP BY sku_id
) attr ON sku.id = attr.sku_id
LEFT JOIN (
  SELECT
    sku_id,
    collect_set(named_struct(
      'sale_attr_id', cast(sale_attr_id as STRING),
      'sale_attr_value_id', cast(sale_attr_value_id as STRING),
      'sale_attr_name', sale_attr_name,
      'sale_attr_value_name', sale_attr_value_name
    )) AS sale_attrs
  FROM ods_sku_sale_attr_value
  WHERE ds = {date}
  GROUP BY sku_id
) sale_attr ON sku.id = sale_attr.sku_id
WHERE sku.ds = {date};
""").show()

spark.sql(f"""
insert overwrite table dim_coupon_full partition(dt='{date}')
SELECT
    ci.id,
    ci.coupon_name,
    CAST(ci.coupon_type AS STRING) AS coupon_type_code,
    coupon_dic.dic_name AS coupon_type_name,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    CASE CAST(ci.coupon_type AS STRING)
        WHEN '3201' THEN CONCAT('满', ci.condition_amount, '元减', ci.benefit_amount, '元')
        WHEN '3202' THEN CONCAT('满', ci.condition_num, '件打', ci.benefit_discount, ' 折')
        WHEN '3203' THEN CONCAT('减', ci.benefit_amount, '元')
    END AS benefit_rule,
    ci.create_time,
    CAST(ci.range_type AS STRING) AS range_type_code,
    range_dic.dic_name AS range_type_name,
    ci.limit_num,
    ci.taken_count,
    ci.start_time,
    ci.end_time,
    ci.operate_time,
    ci.expire_time
FROM
(
    SELECT
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time
    FROM ods_coupon_info
    WHERE ds='{date}'
) ci
LEFT JOIN (
    SELECT
        id,
        dic_name
    FROM ods_base_dic
    WHERE ds='{date}'
      AND parent_code='32'
) coupon_dic ON ci.coupon_type = coupon_dic.id
LEFT JOIN (
    SELECT
        id,
        dic_name
    FROM ods_base_dic
    WHERE ds='{date}'
      AND parent_code='33'
) range_dic ON ci.range_type = range_dic.id;
""")

spark.sql(f"""
insert overwrite table dim_activity_full partition(dt='{date}')
select
    rule.id,
    info.id,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
    end benefit_rule,
    benefit_level
from
(
    select
        id,
        activity_id,
        activity_type,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level
    from ods_activity_rule
    where ds='{date}'
)rule
left join
(
    select
        id,
        activity_name,
        activity_type,
        activity_desc,
        start_time,
        end_time,
        create_time
    from ods_activity_info
    where ds='{date}'
)info
on rule.activity_id=info.id
left join
(
    select
        id,
        dic_name
    from ods_base_dic
    where ds='20250705'
    and parent_code='31'
)dic
on rule.activity_type=dic.id;
""")


spark.sql(f"""
insert overwrite table dim_province_full partition(dt='{date}')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
(
    select
        id,
        name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods_base_province
    where ds='{date}'
)province
left join
(
    select
        id,
        region_name
    from ods_base_region
    where ds='{date}'
)region
on province.region_id=region.id;
""")


spark.sql(f"""

insert overwrite table dim_user_zip partition (dt = '{date}')
select id,
       concat(substr(name, 1, 1), '*')       as         name,
       if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(phone_num, 1, 3), '*'), null) as phone_num,
       if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(email, '@')[1]), null) as  email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       '{date}'                                        start_date,
       '9999-12-31'                                        end_date
from ods_user_info
where ds = '{date}';

""")


spark.sql("SET hive.exec.dynamic.partition = true;")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict;")



spark.sql(f"""
INSERT OVERWRITE TABLE dim_user_zip PARTITION (dt)
SELECT
    id,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date,
    CASE
        WHEN rn = 1 THEN '9999-12-31'
        ELSE '{date}'
    END AS dt
FROM (
    SELECT
        id,
        -- 脱敏处理
        CASE WHEN name IS NOT NULL THEN CONCAT(SUBSTRING(name, 1, 1), '*') ELSE NULL END AS name,
        CASE
            WHEN phone_num RLIKE '^1[3-9][0-9]{9}$' THEN CONCAT(SUBSTRING(phone_num, 1, 3), '****', SUBSTRING(phone_num, 8))
            ELSE NULL
        END AS phone_num,
        CASE
            WHEN email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$' THEN CONCAT('***@', SPLIT(email, '@')[1])
            ELSE NULL
        END AS email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        '{date}' AS start_date,
        CASE
            WHEN rn = 1 THEN '9999-12-31'
            ELSE '{date}'
        END AS end_date,
        rn
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_date DESC) AS rn
        FROM (
            -- 合并变更数据 + 历史快照
            SELECT
          CAST(id AS STRING) AS id,
  CAST(name AS STRING) AS name,
  CAST(phone_num AS STRING) AS phone_num,
  CAST(email AS STRING) AS email,
  CAST(user_level AS STRING) AS user_level,
  CAST(birthday AS STRING) AS birthday,
  CAST(gender AS STRING) AS gender,
  CAST(create_time AS STRING) AS create_time,
  CAST(operate_time AS STRING) AS operate_time,
  '20250705' AS start_date,
  '9999-12-31' AS end_date
            FROM ods_user_info
            WHERE ds = '20250705'

            UNION ALL

            SELECT
                id,
                name,
                phone_num,
                email,
                user_level,
                birthday,
                gender,
                create_time,
                operate_time,
                start_date,
                end_date
            FROM dim_user_zip
            WHERE dt = '9999-12-31'
        ) merged_data
    ) deduped_data
) final_data;
""")
spark.stop()
