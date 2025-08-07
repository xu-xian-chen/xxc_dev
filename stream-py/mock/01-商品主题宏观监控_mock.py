import pymysql
import uuid
import random
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm

fake = Faker()

# ========== 参数配置 ==========
DATE = "2025-08-07"            # 生成数据的日期（yyyy-mm-dd）
DB_CONFIG = {
    "host": "cdh01",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "gmall_log",
    "charset": "utf8mb4",
}

# 维度数据规模
NUM_USERS = 1000
NUM_PRODUCTS = 1000
NUM_ACTIVITIES = 200

# 订单数据量（订单数，订单状态多条记录会更多）
ORDER_MIN = 100000
ORDER_MAX = 120000

# 用户行为日志条数
BEHAVIOR_LOG_MIN = 200000
BEHAVIOR_LOG_MAX = 300000

# 比例参数
REFUND_RATIO = 0.1               # 退款订单占比
PRE_SALE_RATIO = 0.1             # 预售订单占比
ON_SALE_RATIO = 0.9              # 商品在售占比（10%商品下架）
BUY_OFF_SALE_PROB = 0.1          # 下架商品被买概率
PAYMENT_METHODS = ["支付宝", "微信"]
PAYMENT_METHODS_RATIO = [0.6, 0.4]  # 支付宝60%，微信40%
PC_RATIO = 0.3                   # PC端占比，剩余无线端
COD_RATIO = 0.1                  # 货到付款比例

# 订单状态流转
ORDER_STATUS_FLOW = ["已下单", "已支付", "已发货", "已收货", "已退款"]

# 用户行为顺序及可跳过行为（view → collect → cart → order → pay）
BEHAVIOR_TYPES = ["view", "collect", "cart", "order", "pay"]

# 每批插入大小
BATCH_SIZE = 2000

# 时间格式
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


# ========== 工具函数 ==========
def get_uuid():
    return str(uuid.uuid4())

def weighted_choice(choices, weights):
    """根据权重选择"""
    total = sum(weights)
    r = random.uniform(0, total)
    upto = 0
    for c, w in zip(choices, weights):
        if upto + w >= r:
            return c
        upto += w
    return choices[-1]

def random_datetime_within_day(base_date_str):
    base_date = datetime.strptime(base_date_str, "%Y-%m-%d")
    random_seconds = random.randint(0, 86399)
    return base_date + timedelta(seconds=random_seconds)

def datetime_to_str(dt):
    return dt.strftime(TIME_FORMAT)


# ========== 数据库连接 ==========
def get_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        charset=DB_CONFIG["charset"],
        autocommit=True
    )


# ========== 维度数据生成 ==========
def generate_users():
    users = []
    for _ in range(NUM_USERS):
        user_id = get_uuid()
        users.append(user_id)
    return users

def generate_products():
    products = []
    categories = ["家电/电视", "数码/手机", "服装/男装", "食品/零食", "美妆/护肤"]
    for _ in range(NUM_PRODUCTS):
        product_id = get_uuid()
        product_name = fake.word() + fake.word()
        price = round(random.uniform(10, 1000), 2)
        class_path = random.choice(categories)
        class_id = get_uuid()
        is_active = 1 if random.random() < ON_SALE_RATIO else 0
        create_time = datetime_to_str(random_datetime_within_day(DATE))
        modify_time = datetime_to_str(random_datetime_within_day(DATE))
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "price": price,
            "class_id": class_id,
            "class_path": class_path,
            "is_active": is_active,
            "create_time": create_time,
            "modify_time": modify_time,
        })
    return products

def generate_activities(products):
    activities = []
    activity_types = ["discount", "flash_sale"]
    for _ in range(NUM_ACTIVITIES):
        activity_id = get_uuid()
        activity_name = fake.word() + "活动"
        product = random.choice(products)
        product_id = product["product_id"]
        activity_type = random.choice(activity_types)
        start_time = datetime_to_str(random_datetime_within_day(DATE))
        end_time = datetime_to_str(datetime.strptime(start_time, TIME_FORMAT) + timedelta(days=random.randint(1, 10)))
        discount_rate = round(random.uniform(0.5, 0.95), 2)
        create_time = datetime_to_str(random_datetime_within_day(DATE))
        activities.append({
            "activity_id": activity_id,
            "activity_name": activity_name,
            "product_id": product_id,
            "activity_type": activity_type,
            "start_time": start_time,
            "end_time": end_time,
            "discount_rate": discount_rate,
            "create_time": create_time,
        })
    return activities


# ========== 批量插入工具 ==========
def batch_insert(cursor, sql, data):
    """分批批量插入"""
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        cursor.executemany(sql, batch)


# ========== 订单和订单明细生成 ==========
def generate_orders(users, products, activities):
    order_count = random.randint(ORDER_MIN, ORDER_MAX)
    orders_info = []
    order_details = []
    refund_records = []

    for _ in tqdm(range(order_count), desc="生成订单"):
        order_id = get_uuid()
        user_id = random.choice(users)

        # 订单创建时间均匀分布在指定日期
        order_create_time = random_datetime_within_day(DATE)
        terminal_type = "PC" if random.random() < PC_RATIO else "无线"
        is_pre_sale = 1 if random.random() < PRE_SALE_RATIO else 0
        is_cod = 1 if random.random() < COD_RATIO else 0
        payment_method = None if is_cod else weighted_choice(PAYMENT_METHODS, PAYMENT_METHODS_RATIO)

        # 至少2个明细，最多4个，商品不重复
        detail_num = random.randint(2, 4)
        chosen_products = random.sample(products, k=detail_num)

        total_amount = 0.0
        details_for_order = []

        # 生成订单明细
        for product in chosen_products:
            # 商品是否可买（下架商品随机买）
            if product["is_active"] == 0 and random.random() > BUY_OFF_SALE_PROB:
                # 换一个可买商品
                while True:
                    candidate = random.choice(products)
                    if candidate["is_active"] == 1 or random.random() < BUY_OFF_SALE_PROB:
                        product = candidate
                        break

            item_count = random.randint(1, 5)
            item_price = product["price"]
            payment_amount = round(item_price * item_count, 2)
            total_amount += payment_amount

            detail_id = get_uuid()
            class_id = product["class_id"]
            activity = random.choice(activities) if random.random() < 0.2 else None
            activity_id = activity["activity_id"] if activity else None

            details_for_order.append((
                detail_id,
                order_id,
                product["product_id"],
                user_id,
                payment_amount,
                item_count,
                item_price,
                is_pre_sale,
                class_id,
                activity_id,
                datetime_to_str(order_create_time)
            ))

        total_amount = round(total_amount, 2)
        actual_payment = total_amount

        # 付款时间和订单状态流转时间模拟
        if is_cod:
            payment_time = None
        else:
            payment_time_dt = order_create_time + timedelta(minutes=random.randint(1, 30))
            payment_time = datetime_to_str(payment_time_dt)

        # 发货时间
        if payment_time:
            ship_time_dt = payment_time_dt + timedelta(days=random.randint(1, 3))
        else:
            ship_time_dt = order_create_time + timedelta(days=random.randint(1, 3))
        ship_time = datetime_to_str(ship_time_dt)

        # 收货时间
        receive_time_dt = ship_time_dt + timedelta(days=random.randint(1, 5))
        receive_time = datetime_to_str(receive_time_dt)

        # 是否退款
        is_refund = random.random() < REFUND_RATIO
        refund_time = None
        refund_type = None

        # 订单状态多条记录
        order_status_records = []

        # 记录“已下单”
        current_time = order_create_time
        order_status_records.append({
            "order_id": order_id,
            "user_id": user_id,
            "total_amount": total_amount,
            "actual_payment": actual_payment,
            "payment_time": payment_time or "",
            "create_time": datetime_to_str(current_time),
            "terminal_type": terminal_type,
            "order_status": "已下单",
            "is_pre_sale": is_pre_sale,
            "payment_method": payment_method or "",
            "is_cod": is_cod,
            "confirm_time": None
        })

        # 非货到付款，写“已支付”
        if not is_cod:
            current_time = payment_time_dt
            order_status_records.append({
                "order_id": order_id,
                "user_id": user_id,
                "total_amount": total_amount,
                "actual_payment": actual_payment,
                "payment_time": payment_time,
                "create_time": datetime_to_str(current_time),
                "terminal_type": terminal_type,
                "order_status": "已支付",
                "is_pre_sale": is_pre_sale,
                "payment_method": payment_method,
                "is_cod": is_cod,
                "confirm_time": None
            })

        # 已发货
        current_time = ship_time_dt
        order_status_records.append({
            "order_id": order_id,
            "user_id": user_id,
            "total_amount": total_amount,
            "actual_payment": actual_payment,
            "payment_time": payment_time or "",
            "create_time": datetime_to_str(current_time),
            "terminal_type": terminal_type,
            "order_status": "已发货",
            "is_pre_sale": is_pre_sale,
            "payment_method": payment_method or "",
            "is_cod": is_cod,
            "confirm_time": None
        })

        # 已收货
        current_time = receive_time_dt
        confirm_time_str = datetime_to_str(current_time) if is_cod else None
        order_status_records.append({
            "order_id": order_id,
            "user_id": user_id,
            "total_amount": total_amount,
            "actual_payment": actual_payment,
            "payment_time": payment_time or "",
            "create_time": datetime_to_str(current_time),
            "terminal_type": terminal_type,
            "order_status": "已收货",
            "is_pre_sale": is_pre_sale,
            "payment_method": payment_method or "",
            "is_cod": is_cod,
            "confirm_time": confirm_time_str
        })

        # 退款处理
        if is_refund:
            refund_time_dt = receive_time_dt + timedelta(days=random.randint(1, 5))
            refund_time = datetime_to_str(refund_time_dt)
            refund_type = random.choice(["only_refund", "return_refund"])

            # 添加退款订单状态
            current_time = refund_time_dt
            order_status_records.append({
                "order_id": order_id,
                "user_id": user_id,
                "total_amount": total_amount,
                "actual_payment": actual_payment,
                "payment_time": payment_time or "",
                "create_time": datetime_to_str(current_time),
                "terminal_type": terminal_type,
                "order_status": "已退款",
                "is_pre_sale": is_pre_sale,
                "payment_method": payment_method or "",
                "is_cod": is_cod,
                "confirm_time": confirm_time_str
            })

            # 退款记录
            refund_id = get_uuid()
            refund_amount = actual_payment
            refund_records.append((
                refund_id,
                order_id,
                None,
                refund_amount,
                refund_time,
                refund_type,
                is_cod,
                refund_time
            ))

        # 追加订单明细到全局列表
        order_details.extend(details_for_order)

        # 追加订单状态记录到全局列表
        for record in order_status_records:
            orders_info.append((
                record["order_id"],
                record["user_id"],
                record["total_amount"],
                record["actual_payment"],
                record["payment_time"],
                record["create_time"],
                record["terminal_type"],
                record["order_status"],
                record["is_pre_sale"],
                record["payment_method"],
                record["is_cod"],
                record["confirm_time"]
            ))

    return orders_info, order_details, refund_records




# ========== 用户行为日志生成 ==========
def generate_behavior_logs(users, products, orders_info):
    behavior_logs = []
    behavior_count = random.randint(BEHAVIOR_LOG_MIN, BEHAVIOR_LOG_MAX)

    # 建立用户->订单映射
    user_order_map = {}
    for order in orders_info:
        order_id, user_id_, _, _, _, create_time_str, _, order_status = order[0], order[1], order[2], order[3], order[4], order[5], order[6], order[7]
        if order_status == "已下单":
            user_order_map.setdefault(user_id_, []).append(
                (order_id, datetime.strptime(create_time_str, TIME_FORMAT))
            )

    user_sessions = {user: [get_uuid() for _ in range(random.randint(3, 10))] for user in users}

    for i in tqdm(range(behavior_count), desc="生成行为日志"):
        user_id = random.choice(users)
        session_id = random.choice(user_sessions[user_id])
        terminal_type = "PC" if random.random() < PC_RATIO else "无线"

        base_time = random_datetime_within_day(DATE)

        user_behavior_sequence = [b for b in BEHAVIOR_TYPES if b == "view" or random.random() < 0.7]

        current_time = base_time

        for behavior_type in user_behavior_sequence:
            product = random.choice(products)
            product_id = product["product_id"]
            class_id = product["class_id"]

            page_type = random.choice(["detail_page", "micro_detail"])
            click_count = random.randint(1, 5)
            is_jump = 1 if random.random() < 0.1 else 0
            stay_duration = random.randint(1, 300)
            behavior_time_str = datetime_to_str(current_time)

            # 订单关联行为处理
            if behavior_type in ("order", "pay"):
                user_orders = user_order_map.get(user_id, [])
                if user_orders:
                    chosen_order_id, order_create_dt = random.choice(user_orders)
                    if behavior_type == "order":
                        behavior_time_str = datetime_to_str(order_create_dt)
                    else:  # pay
                        pay_dt = order_create_dt + timedelta(minutes=random.randint(1, 60))
                        behavior_time_str = datetime_to_str(pay_dt)

            log_id = get_uuid()

            behavior_logs.append((
                log_id,
                session_id,
                user_id,
                product_id,
                behavior_type,
                behavior_time_str,
                stay_duration,
                terminal_type,
                page_type,
                class_id,
                click_count,
                is_jump,
                datetime_to_str(datetime.now()),
            ))

            current_time += timedelta(seconds=stay_duration + random.randint(1, 10))



    return behavior_logs


# ========== 数据插入 ==========
from tqdm import tqdm

def batch_insert(cursor, sql, data, desc=None):
    """带进度条的分批批量插入"""
    total = len(data)
    with tqdm(total=total, desc=desc, unit="条") as pbar:
        for i in range(0, total, BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]
            cursor.executemany(sql, batch)
            pbar.update(len(batch))


def insert_all_data(users, products, activities, orders_info, order_details, refund_records, behavior_logs):
    conn = get_connection()
    cursor = conn.cursor()

    print("开始插入维度数据...")

    # 插入商品
    sql_product = """
    INSERT INTO product_base_info
    (product_id, product_name, price, class_id, class_path, is_active, create_time, modify_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    product_data = [(p["product_id"], p["product_name"], p["price"], p["class_id"], p["class_path"],
                    p["is_active"], p["create_time"], p["modify_time"]) for p in products]
    batch_insert(cursor, sql_product, product_data, desc="插入商品数据")

    # 插入活动
    sql_activity = """
    INSERT INTO promotion_activity
    (activity_id, activity_name, product_id, activity_type, start_time, end_time, discount_rate, create_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    activity_data = [(a["activity_id"], a["activity_name"], a["product_id"], a["activity_type"], a["start_time"],
                      a["end_time"], a["discount_rate"], a["create_time"]) for a in activities]
    batch_insert(cursor, sql_activity, activity_data, desc="插入活动数据")

    print("开始插入订单相关数据...")

    # order_info
    sql_order_info = """
    INSERT INTO order_info
    (order_id, user_id, total_amount, actual_payment, payment_time, create_time, terminal_type, order_status,
    is_pre_sale, payment_method, is_cod, confirm_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch_insert(cursor, sql_order_info, orders_info, desc="插入订单状态记录")

    # order_detail
    sql_order_detail = """
    INSERT INTO order_detail
    (detail_id, order_id, product_id, user_id, payment_amount, item_count, item_price, is_pre_sale,
    class_id, activity_id, create_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch_insert(cursor, sql_order_detail, order_details, desc="插入订单明细")

    # refund_record
    if refund_records:
        sql_refund = """
        INSERT INTO refund_record
        (refund_id, order_id, product_id, refund_amount, refund_time, refund_type, is_cod, create_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch_insert(cursor, sql_refund, refund_records, desc="插入退款记录")
    else:
        print("无退款记录")

    print("开始插入用户行为日志...")

    # product_behavior_log
    sql_behavior = """
    INSERT INTO product_behavior_log
    (log_id, session_id, user_id, product_id, behavior_type, behavior_time, stay_duration, terminal_type,
    page_type, class_id, click_count, is_jump, create_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch_insert(cursor, sql_behavior, behavior_logs, desc="插入用户行为日志")

    cursor.close()
    conn.close()
    print("数据插入完成！")



# ========== 主流程 ==========
def main():
    print(f"开始生成 {DATE} 的模拟数据...")

    users = generate_users()
    products = generate_products()
    activities = generate_activities(products)

    orders_info, order_details, refund_records = generate_orders(users, products, activities)

    behavior_logs = generate_behavior_logs(users, products, orders_info)

    insert_all_data(users, products, activities, orders_info, order_details, refund_records, behavior_logs)


if __name__ == "__main__":
    main()
