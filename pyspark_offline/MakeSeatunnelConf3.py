import argparse
from struct import pack_into

import pymysql
from pymysql import Error
from datetime import datetime


def get_all_tables(host, port, user, password, database):
    """获取MySQL数据库中所有表名"""
    try:
        connection = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # 获取所有非系统表
        cursor.execute(f"SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        return tables

    except Error as e:
        print(f"获取MySQL表列表失败: {e}")
        exit(1)
    finally:
        if connection:
            connection.close()


def get_table_columns(host, port, user, password, database, table):
    """获取MySQL表的所有字段及其类型"""
    try:
        connection = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
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


def generate_hive_ddl(table, columns,location_url ,output_dir="ddl_scripts"):
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

    ddl_file = os.path.join(output_dir, f"ods_{table}.sql")

    # 生成Hive DDL语句
    hive_ddl = f"-- 自动生成的Hive DDL脚本\n"
    hive_ddl += f"use mouth;\n"
    hive_ddl += f"-- 源表: gmall.{table}\n"
    hive_ddl += f"-- 目标表: ods_{table}\n"
    hive_ddl += f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    hive_ddl += f"CREATE TABLE IF NOT EXISTS ods_{table} (\n"

    # 添加字段定义
    for col in columns:
        hive_type = map_mysql_to_hive_type(col['type'])
        hive_ddl += f"    {col['name']} {hive_type},\n"

    hive_ddl = hive_ddl.rstrip(",\n") + "\n)\n"

    # 添加存储格式和压缩
    hive_ddl +="PARTITIONED BY (ds STRING)\n"
    hive_ddl +=location_url
    hive_ddl += """
    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    """

    # 写入文件
    with open(ddl_file, "w") as f:
        f.write(hive_ddl)

    print(f"  → Hive DDL已生成: {ddl_file}")
    return hive_ddl

def has_create_time_column(columns):
    """检查字段列表中是否包含create_time字段"""
    for col in columns:
        if col['name'].lower() == 'create_time':
            return True
    return False

def generate_seatunnel_config(mysql_host, mysql_port, mysql_user, mysql_password,
                              hive_metastore_uri, hdfs_namenode, table, columns,
                              database="gmall"):
    """
    生成SeaTunnel配置文件

    参数:
    mysql_host: MySQL服务器地址
    mysql_port: MySQL端口
    mysql_user: MySQL用户名
    mysql_password: MySQL密码
    hive_metastore_uri: Hive Metastore URI
    hdfs_namenode: HDFS NameNode地址
    table: 要同步的表名
    columns: 表字段信息
    database: MySQL数据库名
    """

    output_file = f"{table}_to_hive.conf"

    # 生成字段列表
    column_names = [col['name'] for col in columns]
    column_name = [f"`{col['name']}`" for col in columns]
    columns_list = ", ".join(column_name)

    #JDBC查询语句

    has_create_time = has_create_time_column(columns)


    query = f"SELECT {columns_list},date_format(now(),'%Y%m%d') as ds FROM `{database}`.`{table}`"
    # 表名映射关系
    table_mapping = f'        "{table}" = "ods_{table}",'

    config_content = f"""# 自动生成的SeaTunnel配置文件
# 源表: {database}.{table}
# 目标表: ods_{table}
# 生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
# 提示: Hive DDL已单独生成到ddl_scripts目录

env {{
  execution.parallelism = 1
  job.mode = "BATCH"
}}

source {{
  Jdbc {{
    url = "jdbc:mysql://{mysql_host}:{mysql_port}/{database}"
    driver = "com.mysql.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "{mysql_user}"
    password = "{mysql_password}"
    table_path = "{database}.{table}"
    query = "{query}"

  }}
}}

transform {{
 
}}

sink {{
  Hive {{
    table_name = "{database}.ods_{table}"
    metastore_uri = "{hive_metastore_uri}"
    hive.hadoop.conf-path = "/etc/hadoop/conf"
    save_mode = "overwrite"
    partition_by = ["ds"]
    dynamic_partition = true
    file_format = "orc"
    orc_compress = "SNAPPY"
    tbl_properties = {{
            "external.table.purge" = "true"
        }}
    fields = {column_names}
  }}
}}
"""
    import os
    if not os.path.exists("conf"):
        os.makedirs("conf")
        print(f"创建目录: conf")
    ddl_file = os.path.join("conf", f"{output_file}")
    with open(ddl_file, "w") as f:
        f.write(config_content)

    print(f"  → 配置文件已生成: {output_file}")





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='生成SeaTunnel MySQL到Hive同步配置文件')
    parser.add_argument('--mysql_user', default='root', help='MySQL用户名 (默认: root)')
    parser.add_argument('--ddl_dir', default='ddl_scripts', help='DDL文件输出目录 (默认: ddl_scripts)')

    # 使用固定值
    mysql_host = "cdh01"
    mysql_port = "3306"
    hdfs_port = "8020"
    mysql_password = "123456"
    hive_metastore = "thrift://cdh01:9083"
    hdfs_nn = "hdfs://cdh01:8020"
    database = "mouth"
    HDFS_FILE_PATH=f"/warehouse/dmp_ad/2025-06-20/{database}"


    args = parser.parse_args()


    # 获取所有表名
    print("正在获取gmall数据库表列表...")
    tables = get_all_tables(
        host=mysql_host,
        port=mysql_port,
        user=args.mysql_user,
        password=mysql_password,
        database=database
    )

    print(f"找到 {len(tables)} 张表: {', '.join(tables[:3])}..." + (f"等{len(tables)}张表" if len(tables) > 3 else ""))

    # 为每张表生成配置文件和DDL
    for table in tables:
        print(f"\n处理表: {table}")

        # 获取表字段信息
        columns = get_table_columns(
            host=mysql_host,
            port=mysql_port,
            user=args.mysql_user,
            password=mysql_password,
            database=database,
            table=table
        )

        if not columns:
            print(f"  → 警告: 未获取到字段信息，跳过此表")
            continue

        print(f"  → 找到 {len(columns)} 个字段")
        location_url = f"LOCATION '{hdfs_nn}/{HDFS_FILE_PATH}/ods_{table}'\n"
        # 生成Hive DDL文件（单独输出）
        generate_hive_ddl(
            table=table,
            columns=columns,
            location_url=location_url,
            output_dir=args.ddl_dir
        )

        # 生成SeaTunnel配置文件
        generate_seatunnel_config(
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            mysql_user=args.mysql_user,
            mysql_password=mysql_password,
            hive_metastore_uri=hive_metastore,
            hdfs_namenode=hdfs_nn,
            table=table,
            columns=columns,
            database=database
        )

    print("\n所有文件生成完成!")
    print(f"配置文件: 当前目录下的 <表名>_to_hive.conf")
    print(f"Hive DDL: {args.ddl_dir}目录下的 ods_<表名>.sql")
