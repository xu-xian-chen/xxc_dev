create database if not exists yraffic_page;
use yraffic_page;

drop table if exists dim_shop_full;
create table if not exists dim_shop_full
(
    shop_id string comment "店铺id",
    shop_name string comment "店铺名字",
    create_time string comment "创建时间"
)comment '店铺表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/dim/dim_shop_full'
tblproperties('orc.compress'='snappy');

--板块维度表
drop table if exists dim_section_full;
create table if not exists dim_section_full
(
    section_id string comment "板块id",
    section_name string comment "板块名字",
    shop_id string comment "所属商店id",
    create_time string comment "创建时间"
)comment '板块表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/dim/dim_section_full'
tblproperties('orc.compress'='snappy');


--商品维度表
drop table if exists dim_spu_info_full;
create table if not exists dim_spu_info_full
(
    spu_id string comment "商品id",
    spu_name string comment "商品名字",
    amount double comment "商品价格",
    section_id string comment "所属板块id",
    create_time string comment "创建时间"
)comment '商品表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/dim/dim_spu_info_full'
tblproperties('orc.compress'='snappy');

--用户表使用拉链表
drop table if exists dim_user_zip;
create external table dim_user_zip(
    user_id string comment "用户id",
    user_name string comment "用户名字",
    user_num string comment "用户电话",
    user_birthday string comment "生日",
    user_gender string comment "性别",
    region string comment "地区",
    start_date string COMMENT '起始日期',
    end_date string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');