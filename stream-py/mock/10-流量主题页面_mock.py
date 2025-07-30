import pymysql
import random
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
import uuid  # 新增导入uuid

fake = Faker("zh_CN")

# 参数配置


# 格式化成 'YYYYMMDD' 格式的字符串
now = datetime.now()
date_str = now.strftime('%Y%m%d')

TARGET_DATE = date_str
USER_COUNT = random.randint(1000, 5000)           # 1000 - 5000
SHOP_COUNT = random.randint(15, 25)               # 15 - 25
SECTIONS_PER_SHOP = random.randint(5, 8)          # 5 - 8
SPUS_PER_SECTION = random.randint(10, 30)         # 10 - 30
TOTAL_LOGS = random.randint(100000, 300000)       # 100000 - 300000
during_time = random.randint(10, 200)             #10  -   200

def get_connection():
    return pymysql.connect(
        host="cdh01",            # 主机地址
        port=3306,               # MySQL端口
        user="root",             # 数据库用户名
        password="123456",       # 数据库密码
        database="yraffic_page", # 目标数据库
        charset="utf8mb4"
    )

def random_time(date_str):
    base = datetime.strptime(date_str, "%Y-%m-%d")
    rand_seconds = random.randint(0, 86399)
    return (base + timedelta(seconds=rand_seconds)).strftime("%Y-%m-%d %H:%M:%S")

def insert_user_info(conn):
    with conn.cursor() as cursor:
        sql = """
        INSERT INTO user_info (user_id, user_name, user_num, user_birthday, user_gender, region, create_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for _ in tqdm(range(USER_COUNT), desc="插入用户"):
            gender = random.choice(["男", "女"])
            cursor.execute(sql, (
                str(uuid.uuid4()),  # 用uuid替换之前的fake.uuid4()
                fake.name_male() if gender == "男" else fake.name_female(),
                fake.phone_number(),
                fake.date_of_birth(minimum_age=18, maximum_age=60).strftime("%Y-%m-%d"),
                gender,
                fake.province(),
                random_time(TARGET_DATE)
            ))
        conn.commit()

def insert_shop_section_spu(conn):
    shop_ids = []
    section_map = []
    spu_map = []
    with conn.cursor() as cursor:
        shop_sql = "INSERT INTO shop (shop_id, shop_name, create_time) VALUES (%s, %s, %s)"
        section_sql = "INSERT INTO section (section_id, section_name, shop_id, create_time) VALUES (%s, %s, %s, %s)"
        spu_sql = "INSERT INTO spu_info (spu_id, spu_name, amount, section_id, create_time) VALUES (%s, %s, %s, %s, %s)"

        for i in tqdm(range(SHOP_COUNT), desc="插入店铺/板块/商品"):
            shop_id = str(uuid.uuid4())  # 用uuid
            shop_ids.append(shop_id)
            cursor.execute(shop_sql, (shop_id, fake.company(), random_time(TARGET_DATE)))

            for j in range(SECTIONS_PER_SHOP):
                section_id = str(uuid.uuid4())  # 用uuid
                section_map.append((section_id, shop_id))
                cursor.execute(section_sql, (section_id, fake.word(), shop_id, random_time(TARGET_DATE)))

                for k in range(SPUS_PER_SECTION):
                    spu_id = str(uuid.uuid4())  # 用uuid
                    spu_map.append((spu_id, section_id))
                    cursor.execute(spu_sql, (
                        spu_id,
                        fake.word(),
                        round(random.uniform(10, 500), 2),
                        section_id,
                        random_time(TARGET_DATE)
                    ))
        conn.commit()
    return shop_ids, section_map, spu_map

def generate_logs_data(conn, date_str, total_logs):
    print(f"开始生成日志数据：{total_logs} 条，日期为 {date_str}...")

    with conn.cursor() as cursor:
        cursor.execute("SELECT user_id FROM user_info")
        user_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT shop_id FROM shop")
        shop_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT section_id, shop_id FROM section")
        section_map = cursor.fetchall()

        cursor.execute("SELECT spu_id, section_id FROM spu_info")
        spu_map = cursor.fetchall()

    # 构建映射
    section_to_shop = {section_id: shop_id for section_id, shop_id in section_map}
    spu_to_section = {spu_id: section_id for spu_id, section_id in spu_map}
    spu_to_shop = {spu_id: section_to_shop[section_id] for spu_id, section_id in spu_to_section.items()}

    base_time = datetime.strptime(date_str, "%Y-%m-%d")
    interval = 86400 // total_logs

    log_sql = """
        INSERT INTO logs (log_id, user_id, shop_id, section_id, spu_id, page_type, during_time, request_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cursor:
        for i in tqdm(range(total_logs), desc="插入日志"):
            page_type = random.choice(["shop", "section", "spu"])
            user_id = random.choice(user_ids)
            log_id = str(uuid.uuid4())  # 用uuid替换log_id

            request_time = (base_time + timedelta(seconds=i * interval)).strftime("%Y-%m-%d %H:%M:%S")

            shop_id = section_id = spu_id = None

            if page_type == "shop":
                shop_id = random.choice(shop_ids)
            elif page_type == "section":
                section_id, shop_id = random.choice(section_map)
            elif page_type == "spu":
                spu_id, section_id = random.choice(spu_map)
                shop_id = section_to_shop[section_id]

            cursor.execute(log_sql, (
                log_id, user_id, shop_id, section_id, spu_id,
                page_type, during_time, request_time
            ))

            if i % 1000 == 0:
                conn.commit()
        conn.commit()
    print("✅ 日志生成完毕")

def main():
    conn = get_connection()
    try:
        insert_user_info(conn)
        insert_shop_section_spu(conn)
        generate_logs_data(conn, TARGET_DATE, TOTAL_LOGS)
        print("✅ 所有数据生成完成")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
