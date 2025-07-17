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
spark.sql("set hive.vectorized.execution.enabled = false;")


spark.sql("""
insert overwrite table dws_trade_user_sku_order_1d partition(dt)
select
    user_id,
    id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count_1d,
    order_num_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    dt
from
(
    select
        dt,
        user_id,
        sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from dwd_trade_order_detail_inc
    group by dt,user_id,sku_id
)od
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt='{date}'
)sku
on od.sku_id=sku.id;
""")


spark.sql("set hive.vectorized.execution.enabled = false;")
spark.sql("""
insert overwrite table dws_trade_user_sku_order_1d partition(dt='{date}')
select
    user_id,
    id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count,
    order_num,
    order_original_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    order_total_amount
from
(
    select
        user_id,
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(split_original_amount) order_original_amount,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount,
        sum(split_total_amount) order_total_amount
    from dwd_trade_order_detail_inc

    group by user_id,sku_id
)od
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
)sku
on od.sku_id=sku.id;
""")

spark.sql("""
insert overwrite table dws_trade_user_order_1d partition(dt='{date}')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from dwd_trade_order_detail_inc
where dt='{date}'
group by user_id;
""")

spark.sql("""
insert overwrite table dws_trade_user_cart_add_1d partition(dt='{date}')
select
    user_id,
    count(*),
    sum(sku_num)
from dwd_trade_cart_add_inc
group by user_id;
""")

spark.sql("""
insert overwrite table dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd_trade_pay_detail_suc_inc
group by user_id,dt;
""")

spark.sql("""
insert overwrite table dws_trade_province_order_1d partition(dt)
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    dt
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d,
        dt
    from dwd_trade_order_detail_inc
    group by province_id,dt
)o
left join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_province_full
    where dt='{date}'
)p
on o.province_id=p.id;
""")

spark.sql("""
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(dt='{date}')
select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count
from
(
    select
        user_id,
        coupon_id,
        count(*) used_count
    from dwd_tool_coupon_used_inc
    group by user_id,coupon_id
)t1
left join
(
    select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
)t2
on t1.coupon_id=t2.id;
""")

spark.sql("""
insert overwrite table dws_interaction_sku_favor_add_1d partition(dt='{date}')
select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    favor_add_count
from
(
    select
        sku_id,
        count(*) favor_add_count
    from dwd_interaction_favor_add_inc
    group by sku_id
)favor
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
)sku
on favor.sku_id=sku.id;
""")

spark.sql("""
insert into table dws_traffic_session_page_view_1d partition(dt='{date}')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
""")

spark.sql("""
insert overwrite table dws_traffic_page_visitor_page_view_1d partition(dt='{date}')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
group by mid_id,brand,model,operate_system,page_id;
""")

spark.sql("""
insert overwrite table dws_trade_user_sku_order_nd partition(dt='{date}')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(dt>=date_add('{date}',-6),order_count_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_num_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
""")

spark.sql("""
insert overwrite table dws_trade_province_order_nd partition(dt='{date}')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('{date}',-6),order_count_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
group by province_id,province_name,area_code,iso_code,iso_3166_2;
""")

spark.sql("""
insert overwrite table dws_trade_user_order_td partition(dt='{date}')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count,
    sum(order_num_1d) order_num,
    sum(order_original_amount_1d) original_amount,
    sum(activity_reduce_amount_1d) activity_reduce_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from dws_trade_user_order_1d
group by user_id;
""")

# 创建历史数据的临时视图
spark.sql(f"""
create or replace temp view user_login_hist as
select user_id,
       login_date_last,
       login_date_first,
       login_count_td
from dws_user_user_login_td
where dt < '{date}'
""")

# 修改后的SQL
spark.sql(f"""
insert overwrite table dws_user_user_login_td partition (dt = '{date}')
select 
    nvl(old.user_id, new.user_id) as user_id,
    if(new.user_id is null, old.login_date_last, '{date}') as login_date_last,
    if(old.login_date_first is null, '{date}', old.login_date_first) as login_date_first,
    nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0) as login_count_td
from user_login_hist old  -- 使用临时视图替代原表
full outer join (
    select user_id, count(*) login_count_1d
    from dwd_user_login_inc
    where dt = '{date}'
    group by user_id
) new on old.user_id = new.user_id;
""")