# -*- coding: utf-8 -*-
import pymysql
import uuid
from datetime import datetime, timedelta
from tqdm import tqdm,trange
import sys
import random

# =======================
# 模拟参数配置区域（随机化）
# =======================

SIMULATION_DATE = '2025-08-09'

# =======================
# 各表模拟数据条数设置（每次运行都不一样）
# =======================
NUM_PRODUCT = random.randint(5000, 10000)          # 商品数量：5k ~ 10k
NUM_USER = random.randint(8000, 12000)             # 用户数量：8k ~ 12k
NUM_ORDER = random.randint(40000, 60000)           # 订单数量：4w ~ 6w
NUM_ORDER_DETAIL = NUM_ORDER * random.randint(2, 4)  # 明细数量：2~4倍订单数
NUM_BEHAVIOR = random.randint(200000, 300000)      # 行为日志数量
NUM_REFUND = random.randint(5000, 6000)           # 退款记录数量
BATCH_SIZE = 500                                   # 批量插入大小

# =======================
# 概率控制参数（动态模拟业务分布）
# =======================
RATIO_PRODUCT_ONSALE = round(random.uniform(0.80, 0.98), 2)        # 上架商品占比
RATIO_DEVICE_MOBILE = round(random.uniform(0.75, 0.95), 2)         # 移动端占比
RATIO_JUMP = round(random.uniform(0.05, 0.20), 2)                  # 跳出率
RATIO_ORDER_PAID = round(random.uniform(0.75, 0.95), 2)            # 支付成功率
RATIO_REFUND_SUCCESS = round(random.uniform(0.90, 1.00), 2)        # 退款成功率
RATIO_REFUND_ONLY_MONEY = round(random.uniform(0.70, 0.90), 2)     # 退款类型中仅退款占比


# ============================
# 工具方法
# ============================

def get_conn():
    return pymysql.connect(
        host='cdh01',
        user='root',
        password='123456',
        database='gmall_log',
        charset='utf8'
    )

def random_time(base_date=SIMULATION_DATE):
    base = datetime.strptime(base_date, '%Y-%m-%d')
    seconds = random.randint(0, 86399)
    return (base + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S')

# 保存订单ID、用户ID、商品ID列表，用于后续引用
order_ids = []
user_ids = []
product_ids = []

# ============================
# 插入商品信息
# ============================

def insert_product_info(n=NUM_PRODUCT):
    tqdm.write("📦 product_info")
    conn = get_conn(); cursor = conn.cursor()
    sql = '''INSERT INTO product_info (
        product_id, product_name, brand_id, brand_name,
        category1_id, category1_name, category2_id, category2_name,
        category3_id, category3_name, price, is_on_sale, create_time
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    brands = [('101','Apple'),('102','Huawei'),('103','Xiaomi')]
    cat1s = [('10','Electronics'),('11','Home')]
    cat2s = [('100','Phone'),('110','TV')]
    cat3s = [('1001','Smartphone'),('1101','LED')]
    buf = []
    for _ in tqdm(range(n)):
        pid = str(uuid.uuid4())[:20]
        product_ids.append(pid)
        data = (
            pid, 'Product_' + pid,
            *random.choice(brands),
            *random.choice(cat1s),
            *random.choice(cat2s),
            *random.choice(cat3s),
            round(random.uniform(10, 5000), 2),
            1 if random.random() < RATIO_PRODUCT_ONSALE else 0,
            random_time()
        )
        buf.append(data)
        if len(buf) >= BATCH_SIZE: cursor.executemany(sql, buf); buf.clear()
    if buf: cursor.executemany(sql, buf)
    conn.commit(); cursor.close(); conn.close()

# ============================
# 插入用户信息
# ============================

def insert_user_info(n=NUM_USER):
    tqdm.write("📦 user_info")
    conn = get_conn(); cursor = conn.cursor()
    sql = '''INSERT INTO user_info (
        user_id, user_name, gender, age, user_level, register_time, source_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    genders = ['male', 'female']
    levels = ['bronze','silver','gold']
    sources = ['organic']*60 + ['ads']*30 + ['referral']*10
    buf = []
    for _ in tqdm(range(n)):
        uid = str(uuid.uuid4())[:20]
        user_ids.append(uid)
        data = (
            uid, 'User_' + uid,
            random.choice(genders),
            str(random.randint(18, 55)),
            random.choice(levels),
            random_time(),
            random.choice(sources)
        )
        buf.append(data)
        if len(buf) >= BATCH_SIZE: cursor.executemany(sql, buf); buf.clear()
    if buf: cursor.executemany(sql, buf)
    conn.commit(); cursor.close(); conn.close()

# ============================
# 插入订单信息
# ============================

def insert_order_info(n=NUM_ORDER):
    tqdm.write("📦 order_info")
    conn = get_conn(); cursor = conn.cursor()
    sql = '''INSERT INTO order_info (
        order_id, user_id, order_status, order_time,
        total_amount, pay_time, pay_type, is_presell
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
    statuses = ['paid']*85 + ['unpaid']*10 + ['refund']*5
    pay_types = ['alipay']*50 + ['wechat']*40 + ['cod']*10
    buf = []
    for _ in tqdm(range(n)):
        oid = str(uuid.uuid4())[:20]
        order_ids.append(oid)
        status = random.choice(statuses)
        data = (
            oid,
            random.choice(user_ids),
            status,
            random_time(),
            round(random.uniform(10, 3000), 2),
            random_time() if status != 'unpaid' else '',
            random.choice(pay_types),
            random.randint(0, 1)
        )
        buf.append(data)
        if len(buf) >= BATCH_SIZE: cursor.executemany(sql, buf); buf.clear()
    if buf: cursor.executemany(sql, buf)
    conn.commit(); cursor.close(); conn.close()

# ============================
# 插入订单明细（真实关联）
# ============================

def insert_order_detail(n=NUM_ORDER_DETAIL):
    tqdm.write("📦 order_detail")

    conn = get_conn(); cursor = conn.cursor()
    sql = '''INSERT INTO order_detail (
        detail_id, order_id, product_id, product_name,
        price, quantity, sku_spec
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    buf = []
    for _ in tqdm(range(n)):
        did = str(uuid.uuid4())[:20]
        oid = random.choice(order_ids)
        pid = random.choice(product_ids)
        data = (
            did,
            oid,
            pid,
            'Product_' + pid,
            round(random.uniform(10, 1000), 2),
            random.randint(1, 3),
            'Color_' + random.choice(['Red', 'Blue']) + '_Size_' + random.choice(['M', 'L'])
        )
        buf.append(data)
        if len(buf) >= BATCH_SIZE: cursor.executemany(sql, buf); buf.clear()
    if buf: cursor.executemany(sql, buf)
    conn.commit(); cursor.close(); conn.close()

# ============================
# 插入行为日志
# ============================

def insert_user_behavior_log(n=NUM_BEHAVIOR):
    tqdm.write("📦 user_behavior_log")
    conn = get_conn(); cursor = conn.cursor()
    sql = '''INSERT INTO user_behavior_log (
        log_id, user_id, product_id, behavior_type,
        device_type, channel, stay_time, is_jump,
        event_time, session_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    behaviors = ['view']*50 + ['click']*20 + ['cart']*15 + ['fav']*10 + ['micro_detail']*5
    devices = ['mobile']*int(RATIO_DEVICE_MOBILE*100) + ['pc']*int((1-RATIO_DEVICE_MOBILE)*100)
    channels = ['organic']*60 + ['ads']*30 + ['direct']*10
    buf = []
    for _ in tqdm(range(n)):
        lid = str(uuid.uuid4())[:20]
        uid = random.choice(user_ids)
        pid = random.choice(product_ids)
        data = (
            lid, uid, pid,
            random.choice(behaviors),
            random.choice(devices),
            random.choice(channels),
            random.randint(5, 300),
            1 if random.random() < RATIO_JUMP else 0,
            random_time(),
            str(uuid.uuid4())[:32]
        )
        buf.append(data)
        if len(buf) >= BATCH_SIZE: cursor.executemany(sql, buf); buf.clear()
    if buf: cursor.executemany(sql, buf)
    conn.commit(); cursor.close(); conn.close()

# ============================
# 插入退款信息（真实关联）
# ============================

def insert_refund_info(n=NUM_REFUND):
    tqdm.write("📦 refund_info")
    sys.stdout.flush()
    conn = get_conn(); cursor = conn.cursor()
    sql = '''INSERT INTO refund_info (
        refund_id, order_id, product_id, refund_type,
        refund_amount, refund_time, is_success
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    refund_types = ['only_money']*int(RATIO_REFUND_ONLY_MONEY*100) + ['return_goods']*int((1-RATIO_REFUND_ONLY_MONEY)*100)
    buf = []
    for _ in tqdm(range(n)):
        rid = str(uuid.uuid4())[:20]
        data = (
            rid,
            random.choice(order_ids),
            random.choice(product_ids),
            random.choice(refund_types),
            round(random.uniform(10, 3000), 2),
            random_time(),
            1 if random.random() < RATIO_REFUND_SUCCESS else 0
        )
        buf.append(data)
        if len(buf) >= BATCH_SIZE: cursor.executemany(sql, buf); buf.clear()
    if buf: cursor.executemany(sql, buf)
    conn.commit(); cursor.close(); conn.close()

# ============================
# 主函数
# ============================

if __name__ == '__main__':
    insert_product_info()
    insert_user_info()
    insert_order_info()
    insert_order_detail()
    insert_user_behavior_log()
    insert_refund_info()
