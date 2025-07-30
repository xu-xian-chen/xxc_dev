create database if not exists yraffic_page;
use yraffic_page;

--用户维度表
drop table if exists ods_user_info_inc;
create external table if not exists ods_user_info_inc
(
    user_id string comment "用户id",
    user_name string comment "用户名字",
    user_num string comment "用户电话",
    user_birthday string comment "生日",
    user_gender string comment "性别",
    region string comment "地区",
    create_time string comment "创建日期"
)comment '用户表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/ods/ods_user_info_inc'
tblproperties('orc.compress'='snappy');

--店铺维度表
drop table if exists ods_shop_full;
create table if not exists ods_shop_full
(
    shop_id string comment "店铺id",
    shop_name string comment "店铺名字",
    create_time string comment "创建时间"
)comment '店铺表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/ods/ods_shop_full'
tblproperties('orc.compress'='snappy');

--板块维度表
drop table if exists ods_section_full;
create table if not exists ods_section_full
(
    section_id string comment "板块id",
    section_name string comment "板块名字",
    shop_id string comment "所属商店id",
    create_time string comment "创建时间"
)comment '板块表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/ods/ods_section_full'
tblproperties('orc.compress'='snappy');


--商品维度表
drop table if exists ods_spu_info_full;
create table if not exists ods_spu_info_full
(
    spu_id string comment "商品id",
    spu_name string comment "商品名字",
    amount double comment "商品价格",
    section_id string comment "所属板块id",
    create_time string comment "创建时间"
)comment '商品表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/ods/ods_spu_info_full'
tblproperties('orc.compress'='snappy');

--事实表
drop table if exists  ods_logs_inc;
create table if not exists ods_logs_inc
(
    log_id     string comment "日志id",
    user_id    string comment "用户id",
    shop_id    string comment "店铺id",
    section_id string comment "板块id",
    spu_id string comment "商品id",
    page_type string comment "页面类型",
    during_time int comment "停留时长(秒)",
    request_time string comment "访问时间"
)comment '用户行为日志事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/page/ods/ods_logs_inc'
tblproperties('orc.compress'='snappy');




