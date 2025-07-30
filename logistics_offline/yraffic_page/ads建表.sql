use yraffic_page;

-- 1. 商品访问次数1天7天30天统计表
drop table if exists ads_spu_visit;
create table if not exists ads_spu_visit
(
    spu_id string comment '商品id',
    spu_name string comment '商品名字',
    visit_count_1d bigint comment '1天访问次数',
    visit_count_7d bigint comment '7天访问次数',
    visit_count_30d bigint comment '30天访问次数'
) comment '商品访问次数1天7天30天统计表'
partitioned by (ds string comment '统计日期（指标计算基准日期）')
stored as orc
location '/warehouse/page/ads/ads_spu_visit'
tblproperties ('orc.compress'='snappy');

-- 2. 板块访问次数1天7天30天统计表
drop table if exists ads_section_visit;
create table if not exists ads_section_visit
(
    section_id string comment '板块id',
    section_name string comment '板块名字',
    visit_count_1d bigint comment '1天访问次数',
    visit_count_7d bigint comment '7天访问次数',
    visit_count_30d bigint comment '30天访问次数'
) comment '板块访问次数1天7天30天统计表'
partitioned by (ds string comment '统计日期')
stored as orc
location '/warehouse/page/ads/ads_section_visit'
tblproperties ('orc.compress'='snappy');

-- 3. 店铺访问次数1天7天30天统计表
drop table if exists ads_shop_visit;
create table if not exists ads_shop_visit
(
    shop_id string comment '店铺id',
    shop_name string comment '店铺名字',
    visit_count_1d bigint comment '1天访问次数',
    visit_count_7d bigint comment '7天访问次数',
    visit_count_30d bigint comment '30天访问次数'
) comment '店铺访问次数1天7天30天统计表'
partitioned by (ds string comment '统计日期')
stored as orc
location '/warehouse/page/ads/ads_shop_visit'
tblproperties ('orc.compress'='snappy');

-- 4. 商品访问占板块访问比例1天7天30天统计表
drop table if exists ads_section_spu_ratio;
create table if not exists ads_section_spu_ratio
(
    section_id string comment '板块id',
    section_name string comment '板块名字',
    spu_id string comment '商品id',
    spu_name string comment '商品名字',
    ratio_1d double comment '1天占比',
    ratio_7d double comment '7天占比',
    ratio_30d double comment '30天占比'
) comment '商品访问占板块访问比例1天7天30天统计表'
partitioned by (ds string comment '统计日期')
stored as orc
location '/warehouse/page/ads/ads_section_spu_ratio'
tblproperties ('orc.compress'='snappy');

-- 5. 板块访问占店铺访问比例1天7天30天统计表
drop table if exists ads_shop_section_ratio;
create table if not exists ads_shop_section_ratio
(
    shop_id string comment '店铺id',
    shop_name string comment '店铺名字',
    section_id string comment '板块id',
    section_name string comment '板块名字',
    ratio_1d double comment '1天占比',
    ratio_7d double comment '7天占比',
    ratio_30d double comment '30天占比'
) comment '板块访问占店铺访问比例1天7天30天统计表'
partitioned by (ds string comment '统计日期')
stored as orc
location '/warehouse/page/ads/ads_shop_section_ratio'
tblproperties ('orc.compress'='snappy');

-- 6. 店铺访问平均停留时长1天7天30天统计表
drop table if exists ads_shop_avg_stay;
create table if not exists ads_shop_avg_stay
(
    shop_id string comment '店铺id',
    shop_name string comment '店铺名字',
    avg_stay_time_1d double comment '1天平均停留时长(秒)',
    avg_stay_time_7d double comment '7天平均停留时长(秒)',
    avg_stay_time_30d double comment '30天平均停留时长(秒)'
) comment '店铺访问平均停留时长1天7天30天统计表'
partitioned by (ds string comment '统计日期')
stored as orc
location '/warehouse/page/ads/ads_shop_avg_stay'
tblproperties ('orc.compress'='snappy');
