use yraffic_page;

insert overwrite table dwd_logs_wide partition(ds = '20250730')
select
    l.log_id,
    l.user_id,
    u.user_name,
    u.user_gender,
    u.user_num,
    u.user_birthday,
    u.region,

    l.shop_id,
    s.shop_name,

    l.section_id,
    se.section_name,

    l.spu_id,
    sp.spu_name,
    sp.amount,

    l.page_type,
    l.during_time,
    l.request_time
from ods_logs_inc l
left join dim_user_zip u on l.user_id = u.user_id and u.ds = '99991231'
left join dim_shop_full s on l.shop_id = s.shop_id and s.ds = '20250730'
left join dim_section_full se on l.section_id = se.section_id and se.ds = '20250730'
left join dim_spu_info_full sp on l.spu_id = sp.spu_id and sp.ds = '20250730'
where l.ds = '20250730';


select count(1) as cnt
from dwd_logs_wide;


