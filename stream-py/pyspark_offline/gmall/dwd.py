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
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")



spark.sql(f"""
insert into table dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    '{date}' as dt
from ods_cart_info
where ds = '{date}';
""").show()


spark.sql(f"""
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
     date_format(create_time, 'yyyy-MM-dd') date_id,
   null as create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
     '{date}' as dt
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.create_time,
        CAST(data.sku_num AS BIGINT) AS sku_num,  -- 关键修复：转换类型
        CAST(data.sku_num AS BIGINT) * data.order_price AS split_original_amount,  -- 更新计算
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail data
    where ds = '{date}'
) od
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info data
    where ds = '{date}'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity data
    where ds = '{date}'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon data
    where ds = '{date}'
) cou
on od.id = cou.order_detail_id;
""")

spark.sql(f"""
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
     date_format(callback_time, 'yyyy-MM-dd') date_id,
    callback_time,
    od.sku_num,  -- 使用转换后的字段
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    '{date}'
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        CAST(data.sku_num AS BIGINT) AS sku_num,  -- 修复：添加类型转换
        CAST(data.sku_num AS BIGINT) * data.order_price AS split_original_amount,  -- 使用转换后的值
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail data
    where ds = '{date}'
) od
join
(
    select
        data.user_id,
        data.order_id,
        data.payment_type,
        data.callback_time
    from ods_payment_info data
    where ds='{date}'
) pi
on od.order_id=pi.order_id
left join
(
    select
        data.id,
        data.province_id
    from ods_order_info data
    where ds = '{date}'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity data
    where ds = '{date}'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon data
    where ds = '{date}'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        id,
        dic_name
    from ods_base_dic
    where ds='{date}'
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.id;
""")

spark.sql(f"""
insert overwrite table dwd_trade_cart_full partition(dt='{date}')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where ds='{date}'
and is_ordered='0';
""")

spark.sql(f"""
insert overwrite table dwd_trade_trade_flow_acc partition(dt)
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    '{date}'
from
(
    select
        data.id,
        data.user_id,
        data.province_id,
        data.create_time,
        data.original_total_amount,
        data.activity_reduce_amount,
        data.coupon_reduce_amount,
        data.total_amount
    from ods_order_info data
    where ds='{date}'
)oi
left join
(
    select
        data.order_id,
        data.callback_time,
        data.total_amount payment_amount
    from ods_payment_info data
    where ds='{date}'

)pi
on oi.id=pi.order_id
left join
(
    select
        data.order_id,
        data.operate_time finish_time
    from ods_order_status_log data
    where ds='{date}'
    and data.order_status='1004'
)log
on oi.id=log.order_id;
""")

spark.sql(f"""
insert overwrite table dwd_tool_coupon_used_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
     date_format(get_time, 'yyyy-MM-dd') date_id,
    data.used_time,
     date_format(get_time, 'yyyy-MM-dd')
from ods_coupon_use data
where ds='{date}';


""")

spark.sql(f"""
insert overwrite table dwd_interaction_favor_add_inc partition(dt)
select
    data.id,
    data.user_id,
    data.sku_id,
     date_format(create_time, 'yyyy-MM-dd') date_id,
    data.create_time,
    '{date}'
from ods_favor_info data
where ds='{date}';

""")