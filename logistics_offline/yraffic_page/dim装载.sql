use yraffic_page;

--1.   板块表
insert into dim_section_full
    partition (ds = "20250730")
select
    section_id,
    section_name,
    shop_id,
    create_time
from ods_section_full
where ds = "20250730";

--2.   商店表
insert  into dim_shop_full
    partition (ds = '20250730')
select
    shop_id,
    shop_name,
    create_time
from ods_shop_full
where ds = '20250730';


--3.  商品表
insert into dim_spu_info_full
    partition (ds = "20250730")
select
    spu_id,
    spu_name,
    amount,
    section_id,
    create_time
from ods_spu_info_full
where ds = "20250730";


--4.  用户拉链表
--4.1  首次装载
insert overwrite table dim_user_zip
    partition (ds = '99991231')
select user_id,
       user_name,
       user_num,
       user_birthday,
       user_gender,
       region,
       substr(create_time, 1, 10) as start_date,
       '99991231' as end_time
from ods_user_info_inc
where ds = '20250730';

--4.2  每日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip
    partition (ds)
select
     user_id,
     user_name,
     user_num,
     user_birthday,
     user_gender,
     region,
     start_date,
     if(rk = 1, end_date, date_add('20250730', -1)) as  end_date,
     if(rk = 1, end_date, date_add('20250730', -1)) as  dt
from
(
    select
        user_id,
        user_name,
        user_num,
        user_birthday,
        user_gender,
        region,
        start_date,
        end_date,
        row_number() over (partition by user_id order by start_date desc) rk
    from
    (
        select
            user_id,
            user_name,
            user_num,
            user_birthday,
            user_gender,
            region,
            start_date,
            end_date
        from dim_user_zip
        where ds = '99991231'
        union
        select
            user_id,
            user_name,
            user_num,
            user_birthday,
            user_gender,
            region,
            '20250730' as start_date,
            '99991231' as end_date
        from
        (
            select user_id,
                   user_name,
                   user_num,
                   user_birthday,
                   user_gender,
                   region,
                   row_number() over (partition by user_id order by ds desc) rn
            from ods_user_info_inc
            where ds = '20250730'
        ) inc
        where rn = 1
    )full_info
);



