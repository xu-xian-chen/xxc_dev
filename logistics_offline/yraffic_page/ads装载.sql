use yraffic_page;


-- 1. 商品访问次数1天7天30天统计表
insert overwrite table dws_spu_visit partition(ds='20250730')
select
    spu_id,
    spu_name,
    sum(if(ds = '20250730', 1, 0)) as visit_count_1d,
    sum(if(ds >= date_format(date_sub('2025-07-30', 6), 'yyyyMMdd') and ds <= '20250730', 1, 0)) as visit_count_7d,
    sum(if(ds >= date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and ds <= '20250730', 1, 0)) as visit_count_30d
from dwd_logs_wide
where ds between date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and '20250730'
    and spu_id is not null
group by spu_id, spu_name;


-- 2. 板块访问次数1天7天30天统计表
insert overwrite table dws_section_visit partition(ds='20250730')
select
    section_id,
    section_name,
    sum(if(ds = '20250730', 1, 0)) as visit_count_1d,
    sum(if(ds >= date_format(date_sub('2025-07-30', 6), 'yyyyMMdd') and ds <= '20250730', 1, 0)) as visit_count_7d,
    sum(if(ds >= date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and ds <= '20250730', 1, 0)) as visit_count_30d
from dwd_logs_wide
where ds between date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and '20250730'
    and section_id is not null
group by section_id, section_name;



-- 3. 店铺访问次数1天7天30天统计表
insert overwrite table dws_shop_visit partition(ds='20250730')
select
    shop_id,
    shop_name,
    sum(if(ds = '20250730', 1, 0)) as visit_count_1d,
    sum(if(ds >= date_format(date_sub('2025-07-30', 6), 'yyyyMMdd') and ds <= '20250730', 1, 0)) as visit_count_7d,
    sum(if(ds >= date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and ds <= '20250730', 1, 0)) as visit_count_30d
from dwd_logs_wide
where ds between date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and '20250730'
    and shop_id is not null
group by shop_id, shop_name;



-- 4. 商品访问占板块访问比例1天7天30天统计表
insert overwrite table dws_section_spu_ratio partition(ds='20250730')
with
-- 设置时间边界
date_bound as (
    select
        '20250730'                                    as end_day,
        date_format(date_sub(`current_date`(), 6), 'yyyyMMdd')  as start_day_7d,
        date_format(date_sub(`current_date`(), 29), 'yyyyMMdd') as start_day_30d
),
-- 板块-商品访问次数汇总（1天、7天、30天）
section_spu_cnt as (
    select
        log.section_id,
        log.spu_id,
        sum(if(log.ds = '20250730', 1, 0)) as spu_count_1d,
        sum(if(log.ds >= d.start_day_7d and log.ds <= d.end_day, 1, 0)) as spu_count_7d,
        sum(if(log.ds >= d.start_day_30d and log.ds <= d.end_day, 1, 0)) as spu_count_30d
    from dwd_logs_wide log
    cross join date_bound d
    where
        log.ds between d.start_day_30d and d.end_day
        and log.spu_id is not null   --过滤spu为空的字段
    group by log.section_id, log.spu_id
),
-- 板块总访问次数（1天、7天、30天）
section_total_cnt as (
    select
        log.section_id,
        sum(if(log.ds = '20250730', 1, 0)) as total_count_1d,
        sum(if(log.ds >= d.start_day_7d and log.ds <= d.end_day, 1, 0)) as total_count_7d,
        sum(if(log.ds >= d.start_day_30d and log.ds <= d.end_day, 1, 0)) as total_count_30d
    from dwd_logs_wide log
    cross join date_bound d
    where
        log.ds between d.start_day_30d and d.end_day
        and log.spu_id is not null  --过滤spu为空的字段
    group by log.section_id
)
select
    s.section_id,
    sec.section_name,
    s.spu_id,
    sp.spu_name,
    round(s.spu_count_1d * 100.0 / nullif(t.total_count_1d, 0), 4) as ratio_1d,
    round(s.spu_count_7d * 100.0 / nullif(t.total_count_7d, 0), 4) as ratio_7d,
    round(s.spu_count_30d * 100.0 / nullif(t.total_count_30d, 0), 4) as ratio_30d
from section_spu_cnt s
left join section_total_cnt t on s.section_id = t.section_id
left join dim_section_full sec on s.section_id = sec.section_id and sec.ds = '20250730'
left join dim_spu_info_full sp on s.spu_id = sp.spu_id and sp.ds = '20250730';



-- 5. 板块访问占店铺访问比例1天7天30天统计表
insert overwrite table dws_shop_section_ratio partition(ds='20250730')
with
date_bound as (
    select
        '20250730' as end_day,
        date_format(date_sub(`current_date`(), 6), 'yyyyMMdd') as start_day_7d,
        date_format(date_sub(`current_date`(), 29), 'yyyyMMdd') as start_day_30d
),
base as (
    select
        log.shop_id,
        log.shop_name,
        log.section_id,
        log.section_name,
        sum(if(log.ds = d.end_day, 1, 0)) as cnt_1d,
        sum(if(log.ds >= d.start_day_7d and log.ds <= d.end_day, 1, 0)) as cnt_7d,
        sum(if(log.ds >= d.start_day_30d and log.ds <= d.end_day, 1, 0)) as cnt_30d
    from dwd_logs_wide log
    cross join date_bound d
    where
        log.ds between d.start_day_30d and d.end_day
        and log.section_id is not null     --过滤section_id为空的
        and log.shop_id is not null        --过滤shop_id为空的
    group by log.shop_id, log.shop_name, log.section_id, log.section_name
),
final as (
    select
        shop_id,
        shop_name,
        section_id,
        section_name,
        round(cnt_1d / nullif(sum(cnt_1d) over (partition by shop_id), 0), 4) as ratio_1d,
        round(cnt_7d / nullif(sum(cnt_7d) over (partition by shop_id), 0), 4) as ratio_7d,
        round(cnt_30d / nullif(sum(cnt_30d) over (partition by shop_id), 0), 4) as ratio_30d
    from base
)
select *
from final
where section_id is not null;




-- 6. 店铺访问平均停留时长1天7天30天统计表
insert overwrite table dws_shop_avg_stay partition(ds='20250730')
select
    shop_id,
    round(
        avg(if(ds = '20250730', during_time, null)),
        2
    ) as avg_stay_time_1d,
    round(
        avg(if(ds >= date_format(date_sub('2025-07-30', 6), 'yyyyMMdd') and ds <= '20250730', during_time, null)),
        2
    ) as avg_stay_time_7d,
    round(
        avg(if(ds >= date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and ds <= '20250730', during_time, null)),
        2
    ) as avg_stay_time_30d
from dwd_logs_wide
where ds between date_format(date_sub('2025-07-30', 29), 'yyyyMMdd') and '20250730'
group by shop_id;


