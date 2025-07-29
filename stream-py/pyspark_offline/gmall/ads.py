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
spark.sql("""
insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel
union
select
    '20250705' dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('20250705',-recent_days+1)
group by recent_days,channel;
""")

spark.sql("""
insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '20250705' dt,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from dwd_traffic_page_view_inc
        where dt='20250705'
    )t1
)t2
group by source,target;
""")

spark.sql("""
insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '20250705' dt,
        count(*) user_churn_count
    from dws_user_user_login_td
    where dt='20250705'
    and login_date_last=date_add('20250705',-7)
)churn
join
(
    select
        '20250705' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where dt='20250705'
        and login_date_last = '20250705'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where dt=date_add('20250705',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.dt=back.dt;
""")

spark.sql("""
insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '20250705' dt,
       recent_days,
       sum(if(login_date_first >= date_add('20250705', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '20250705'
  and login_date_last >= date_add('20250705', -recent_days + 1)
group by recent_days;

""")

spark.sql("""
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '20250705' dt,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws_traffic_page_visitor_page_view_1d
    where dt='20250705'
    and page_id in ('home','good_detail')
)page
join
(
    select
        1 recent_days,
        count(*) cart_count
    from dws_trade_user_cart_add_1d
    where dt='20250705'
)cart
on page.recent_days=cart.recent_days
join
(
    select
        1 recent_days,
        count(*) order_count
    from dws_trade_user_order_1d
    where dt='20250705'
)ord
on page.recent_days=ord.recent_days
join
(
    select
        1 recent_days,
        count(*) payment_count
    from dws_trade_user_payment_1d
    where dt='20250705'
)pay
on page.recent_days=pay.recent_days;
""")

spark.sql("""
insert overwrite table ads_order_continuously_user_count
select * from ads_order_continuously_user_count
union
select
    '20250705',
    7,
    count(distinct(user_id))
from
(
    select
        user_id,
        datediff(lead(dt,2,'20250705') over(partition by user_id order by dt),dt) diff
    from dws_trade_user_order_1d
    where dt>=date_add('20250705',-6)
)t1
where diff=2;
""")

spark.sql("""
insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '20250705',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(order_count_30d) order_count
    from dws_trade_user_sku_order_nd
    where dt='20250705'
    group by user_id, tm_id,tm_name
)t1
group by tm_id,tm_name;
""")

spark.sql("""
insert overwrite table ads_order_stats_by_tm
select * from ads_order_stats_by_tm
union
select
    '20250705' dt,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        tm_id,
        tm_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='20250705'
    group by tm_id,tm_name
    union all
    select
        recent_days,
        tm_id,
        tm_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='20250705'
    )t1
    group by recent_days,tm_id,tm_name
)odr;
""")

spark.sql("""
insert overwrite table ads_order_stats_by_cate
select * from ads_order_stats_by_cate
union
select
    '20250705' dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='20250705'
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='20250705'
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
)odr;
""")

spark.sql("""
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '20250705' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd_trade_cart_full
        where dt='20250705'
        group by sku_id
    )cart
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
            category3_name
        from dim_sku_full
        where dt='20250705'
    )sku
    on cart.sku_id=sku.id
)t1
where rk<=3;
""")

spark.sql("""
insert overwrite table ads_sku_favor_count_top3_by_tm
select * from ads_sku_favor_count_top3_by_tm
union
select
    '20250705' dt,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
(
    select
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rank() over (partition by tm_id order by favor_add_count_1d desc) rk
    from dws_interaction_sku_favor_add_1d
    where dt='20250705'
)t1
where rk<=3;
""")

spark.sql("""
insert overwrite table ads_order_to_pay_interval_avg
select * from ads_order_to_pay_interval_avg
union
select
    '20250705',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd_trade_trade_flow_acc
where dt in ('20250705')
and payment_date_id='20250705';
""")

spark.sql("""
insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
select
    '20250705' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws_trade_province_order_1d
where dt='20250705'
union
select
    '20250705' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='20250705';


""")
spark.stop()
