#!/usr/bin/python
# -*- coding: UTF-8 -*-

import argparse
from datetime import datetime
import pymysql
from pymysql import Error
import time

parser = argparse.ArgumentParser(description='生成SeaTunnel MySQL到Hive同步配置文件')
parser.add_argument('--mysql_user', default='root', help='MySQL用户名 (默认: root)')
parser.add_argument('--ddl_dir', default='ddl_scripts', help='DDL文件输出目录 (默认: ddl_scripts)')

args = parser.parse_args()

mysql_host = "cdh01"
mysql_port = "3306"
mysql_password = "123456"
database = "tms"


hdfs_port = "8020"
hdfs_nn = "hdfs://cdh01:8020"

hive_metastore = "thrift://cdh01:9083"
HDFS_FILE_PATH = f"/warehouse/{database}/ods/"

#全量表集合
SYNC_TYPE_MAP = {
    "a_template_city_distance": "full",
    "base_complex": "full",
    "base_dic": "full",
    "base_organ": "full",
    "base_region_info": "full",
    "employee_info": "full",
    "express_courier": "full",
    "line_base_info": "full",
    "line_base_shift": "full",
    "sorter_info": "full",
    "truck_driver": "full",
    "truck_info": "full",
    "truck_model": "full",
    "truck_team": "full",
    "user_address": "full",
    "user_info": "full",
    "express_task_collect": "inc",
    "express_task_delivery": "inc",
    "order_cargo": "inc",
    "order_info": "inc",
    "order_org_bound": "inc",
    "order_trace_log": "inc",
    "transport_plan_line_detail": "inc",
    "transport_task": "inc",
    "transport_task_detail": "inc",
    "transport_task_process": "inc"
}

def get_mysql_conn():
    connection=None
    try:
        connection = pymysql.connect(
            host=mysql_host,
            port=int(mysql_port),
            user=args.mysql_user,
            password=mysql_password,
            database=database
        )
    except Error as e:
        print(f"mysql连接失败{e}")
    return connection

def get_all_tables():
    """获取MySQL数据库中所有表名"""
    connection=None
    try:
        connection = get_mysql_conn()

        cursor = connection.cursor()

        # 获取所有非系统表
        cursor.execute(f"SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

    except Error as e:
        print(f"获取MySQL表列表失败: {e}")
        exit(1)
    finally:
        connection.close()
    #返回表
    return tables


def get_table_columns(table):
    """获取MySQL表的所有字段及其类型"""
    connection=None
    try:
        connection = get_mysql_conn()

        cursor = connection.cursor()

        # 获取表结构信息
        cursor.execute(f"DESCRIBE `{table}`")
        columns = []
        for row in cursor.fetchall():
            column_info = {
                'name': row[0],
                'type': row[1],
                'nullable': 'YES' in row[2].upper(),
                'key': row[3],
                'default': row[4],
                'extra': row[5]
            }
            columns.append(column_info)

        return columns

    except Error as e:
        print(f"获取表 {table} 的字段信息失败: {e}")
        return []
    finally:
        if connection:
            connection.close()


def map_mysql_to_hive_type(mysql_type):
    """将MySQL数据类型映射到Hive数据类型"""
    mysql_type = mysql_type.lower()

    # 数值类型
    if 'tinyint' in mysql_type:
        return 'TINYINT'
    elif 'smallint' in mysql_type:
        return 'SMALLINT'
    elif 'mediumint' in mysql_type:
        return 'INT'
    elif 'int' in mysql_type or 'integer' in mysql_type:
        return 'INT'
    elif 'bigint' in mysql_type:
        return 'BIGINT'
    elif 'float' in mysql_type:
        return 'FLOAT'
    elif 'double' in mysql_type:
        return 'DOUBLE'
    elif 'decimal' in mysql_type:
        return 'DECIMAL' + mysql_type[mysql_type.find('('):]

    # 日期时间类型
    elif 'date' in mysql_type:
        return 'STRING'
    elif 'time' in mysql_type:
        return 'STRING'
    elif 'timestamp' in mysql_type:
        return 'STRING'
    elif 'year' in mysql_type:
        return 'STRING'

    # 字符串类型
    elif 'char' in mysql_type or 'text' in mysql_type:
        # 处理带长度的字符串类型
        if '(' in mysql_type:
            return 'STRING'
        return 'STRING'
    elif 'binary' in mysql_type or 'blob' in mysql_type:
        return 'BINARY'
    elif 'enum' in mysql_type or 'set' in mysql_type:
        return 'STRING'

    # 其他类型
    else:
        return 'STRING'


def generate_hive_ddl(table,columns,location_url,output_dir="ddl_scripts"):
    """
    生成Hive DDL并保存到单独的文件
    参数:
    table: 表名
    columns: 表字段信息
    output_dir: DDL文件输出目录
    """
    # 确保输出目录存在
    import os
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建目录: {output_dir}")

    table_type = SYNC_TYPE_MAP.get(table)

    ddl_file = os.path.join(output_dir, f"ods_{table}_{table_type}.sql")

    # 生成Hive DDL语句
    hive_ddl = f"-- 自动生成的Hive DDL脚本\n"
    hive_ddl += f"use {database};\n"
    hive_ddl += f"-- 源表: {database}.{table}\n"
    hive_ddl += f"-- 目标表: ods_{table}\n"
    hive_ddl += f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    hive_ddl += f"CREATE TABLE IF NOT EXISTS ods_{table}_{table_type} (\n"

    # 添加字段定义
    for col in columns:
        hive_type = map_mysql_to_hive_type(col['type'])
        hive_ddl += f"    {col['name']} {hive_type},\n"

    hive_ddl = hive_ddl.rstrip(",\n") + "\n)\n"

    # 添加存储格式和压缩
    hive_ddl +="PARTITIONED BY (ds STRING)"
    hive_ddl +=location_url
    hive_ddl += """
    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    """

    # 写入文件
    with open(ddl_file, "w",encoding="utf-8") as f:
        f.write(hive_ddl)

    print(f"  → Hive DDL已生成: {ddl_file}")
    return hive_ddl


def generate_seatunnel_config(table_name,mysql_user,columns):
    """
    生成SeaTunnel配置文件（支持全量/增量表判断）

    参数:
    mysql_host: MySQL服务器地址
    mysql_port: MySQL端口
    mysql_user: MySQL用户名
    mysql_password: MySQL密码
    hive_metastore: Hive Metastore URI
    database: MySQL数据库名
    table_name: 要同步的表名
    columns: 表字段信息
    full_tables_set: 全量表名的集合
    """
    #获得表的类型
    table_type = SYNC_TYPE_MAP.get(table_name)

    output_file = f"{table_type}_{table_name}_to_hive.conf"

    # 生成字段列表
    column_names = [col['name'] for col in columns]
    column_name = [f"`{col['name']}`" for col in columns]
    columns_list = ", ".join(column_name)

    #获取当前时间  格式  20250718
    timestamp = time.time()
    local_time = time.localtime(timestamp)
    formatted_time = time.strftime("%Y%m%d", local_time)

    # 根据表类型生成不同的查询语句
    if table_type=="full":
        # 全量表使用create_time转换成分区字段
        query = f"SELECT {columns_list}, DATE_FORMAT(NOW(), '%Y%m%d') as ds FROM `{database}`.`{table_name}` "
        table_type_note = "全量表 (使用当前日期作为分区)"
    else:
        # 增量表使用当前日期作为分区字段
        query = f"SELECT {columns_list}, DATE_FORMAT(create_time, '%Y%m%d') as ds FROM `{database}`.`{table_name}` "
        query += f"where DATE_FORMAT(create_time, '%Y%m%d')=20250717"
        table_type_note = "增量表 (使用create_time作为分区源)"

    # 目标表名（增加ods前缀）
    target_table = f"ods_{table_name}_{table_type}"

    config_content = f"""
# 自动生成的SeaTunnel配置文件
# 源表: {database}.{table_name} | {table_type_note}
# 目标表: {database}.{target_table}_{table_type}
# 生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

env {{
  execution.parallelism = 1
  job.mode = "BATCH"
}}

source {{
  Jdbc {{
    url = "jdbc:mysql://{mysql_host}:{mysql_port}/{database}?useSSL=false"
    driver = "com.mysql.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "{mysql_user}"
    password = "{mysql_password}"
    query = "{query}"
  }}
}}

transform {{
  # 可在此处添加字段转换规则
}}

sink {{
  Hive {{
    table_name = "{database}.{target_table}"
    metastore_uri = "{hive_metastore}"
    hive.hadoop.conf-path = "/etc/hadoop/conf"
    save_mode = "overwrite"
    partition_by = ["ds"]  # 统一使用ds作为分区字段
    dynamic_partition = true
    file_format = "orc"
    orc_compress = "SNAPPY"
    tbl_properties = {{
      "external.table.purge" = "true"
    }}
    fields = {column_names + ['ds']}  # 包含所有字段+分区字段
  }}
}}
"""
    import os
    output_dir = "conf"
    os.makedirs(output_dir, exist_ok=True)

    config_path = os.path.join(output_dir, output_file)
    with open(config_path, "w",encoding="utf-8") as f:
        f.write(config_content)

    print(f"  → 配置文件已生成: {config_path} [类型: {table_type}]")
    return config_path

def main():


    # 获取所有表名
    print(f"正在获取{database}数据库表列表...")
    tables = get_all_tables()

    print(f"找到 {len(tables)} 张表: {', '.join(tables[:3])}..." + (f"等{len(tables)}张表" if len(tables) > 3 else ""))

    # 为每张表生成配置文件和DDL
    for table1 in tables:
        print(f"\n处理表: {table1}")

        # 获取表字段信息
        columns = get_table_columns(table = table1)

        if not columns:
            print(f"  → 警告: 未获取到字段信息，跳过此表")
            continue

        print(f"  → 找到 {len(columns)} 个字段")
        location_url = f"LOCATION '{hdfs_nn}/{HDFS_FILE_PATH}ods_{table1}_{SYNC_TYPE_MAP.get(table1)}'\n"
        # 生成Hive DDL文件（单独输出）
        generate_hive_ddl(
            table=table1,
            columns=columns,
            location_url=location_url,
            output_dir=args.ddl_dir
        )

        # 生成SeaTunnel配置文件
        generate_seatunnel_config(
            mysql_user=args.mysql_user,
            table_name=table1,
            columns=columns,
        )

    print("\n所有文件生成完成!")
    print(f"配置文件: 当前目录下的 <表名>_to_hive.conf")
    print(f"Hive DDL: {args.ddl_dir}目录下的 ods_<表名>.sql")

if __name__ == "__main__":
   main()




