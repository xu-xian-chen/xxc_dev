use yraffic_page;

drop table if exists dwd_logs_wide;

create table if not exists dwd_logs_wide (
    log_id         string comment "日志id",
--用户维度
    user_id        string comment "用户id",
    user_name      string comment "用户名称",
    user_gender    string comment "用户性别",
    user_num       string comment "手机号",
    user_birthday  string comment "生日",
    region         string comment "省份",
--店铺维度
    shop_id        string comment "店铺id",
    shop_name      string comment "店铺名称",
--板块维度
    section_id     string comment "板块id",
    section_name   string comment "板块名称",
--商品维度
    spu_id         string comment "商品id",
    spu_name       string comment "商品名称",
    amount         decimal(10,2) comment "商品价格",

    page_type      string comment "页面类型",
    during_time    int comment "停留时长（秒）",
    request_time   string comment "访问时间"
)
comment '用户行为宽表'
partitioned by (ds string comment '统计日期')
stored as orc
location '/warehouse/page/dwd/dwd_logs_wide'
tblproperties('orc.compress'='snappy');