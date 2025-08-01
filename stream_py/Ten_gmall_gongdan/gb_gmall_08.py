import pymysql
from faker import Faker
import random
import time
from datetime import datetime, timedelta

# 初始化Faker和随机种子
fake = Faker('zh_CN')
Faker.seed(42)
random.seed(42)

# 数据库配置
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': 'root',
    'database': 'gd_gmall_08',
    'charset': 'utf8mb4'
}

# 客服部门和职位
DEPARTMENTS = ['客服一部', '客服二部', '客服三部', 'VIP客服部', '售后客服部']
POSITIONS = ['客服专员', '高级客服', '客服主管', '客服经理']
OFFER_TYPES = [1, 2, 3]  # 优惠类型
STATUS_VALUES = [0, 1, 2]  # 状态：0-无效，1-有效，2-已使用

# 创建数据库连接
def create_connection():
    try:
        conn = pymysql.connect(**db_config)
        print("数据库连接成功")
        return conn
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        raise

# 批量插入函数
def batch_insert(conn, table_name, columns, data, batch_size=1000):
    if not data:
        print(f"没有数据插入到 {table_name}")
        return

    with conn.cursor() as cursor:
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        query = f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        total_rows = len(data)
        for i in range(0, total_rows, batch_size):
            batch = data[i:i+batch_size]
            try:
                cursor.executemany(query, batch)
                conn.commit()
                print(f"已插入 {min(i+batch_size, total_rows)}/{total_rows} 条数据到 {table_name}")
            except pymysql.MySQLError as e:
                print(f"插入数据失败: {e}")
                conn.rollback()
                raise

# 生成客服信息数据（超过100万条）
def generate_cs_info_data(num_rows):
    data = []
    for i in range(1, num_rows + 1):
        cs_id = i
        cs_name = fake.name()
        department = random.choice(DEPARTMENTS)
        position = random.choice(POSITIONS)
        hire_date = fake.date_between(start_date='-5y', end_date='-30d')
        status = random.choices([0, 1], weights=[0.05, 0.95])[0]  # 5%离职率

        data.append((
            cs_id, cs_name, department, position, hire_date, status
        ))

        # 每10万条打印进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条客服信息数据")

    return data

# 生成客服专属优惠活动数据（超过100万条）
def generate_special_offer_activity(num_rows):
    data = []
    for i in range(1, num_rows + 1):
        activity_id = i
        activity_name = f"{fake.color_name()}专属优惠活动{i}"
        activity_level = random.randint(1, 3)
        offer_type = random.choice(OFFER_TYPES)

        # 活动时间范围覆盖整个30天周期
        start_time = fake.date_time_between(start_date='-30d', end_date='+5d')
        end_time = start_time + timedelta(days=random.randint(15, 60))

        max_custom_amount = round(random.uniform(50, 1000), 2)
        status = random.choices([0, 1], weights=[0.05, 0.95])[0]  # 5%无效活动
        shop_id = random.randint(1, 100)
        create_time = fake.date_time_between(start_date=start_time - timedelta(days=10),
                                             end_date=start_time - timedelta(days=1))
        update_time = fake.date_time_between(start_date=create_time, end_date=start_time)

        data.append((
            activity_id, activity_name, activity_level, offer_type,
            start_time, end_time, max_custom_amount, status,
            shop_id, create_time, update_time
        ))

        # 每10万条打印进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条优惠活动数据")

    return data

# 生成活动商品数据（超过100万条）
def generate_special_offer_products(num_rows, activity_ids):
    data = []
    for i in range(1, num_rows + 1):
        activity_id = random.choice(activity_ids)
        product_id = i
        fixed_offer_amount = round(random.uniform(5, 200), 2)
        max_offer_amount = round(fixed_offer_amount * random.uniform(1.2, 3.0), 2)
        purchase_limit = random.randint(1, 10)
        status = random.choices([0, 1], weights=[0.03, 0.97])[0]  # 3%无效商品
        create_time = fake.date_time_between(start_date='-60d', end_date='-1d')

        data.append((
            activity_id, product_id, fixed_offer_amount,
            max_offer_amount, purchase_limit, status, create_time
        ))

        # 每10万条打印进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条活动商品数据")

    return data

# 生成活动SKU数据（超过100万条）
def generate_special_offer_skus(num_rows, product_ids):
    data = []
    for i in range(1, num_rows + 1):
        product_id = random.choice(product_ids)
        sku_id = i
        fixed_offer_amount = round(random.uniform(5, 200), 2)
        max_offer_amount = round(fixed_offer_amount * random.uniform(1.2, 3.0), 2)
        create_time = fake.date_time_between(start_date='-60d', end_date='-1d')

        data.append((
            product_id, sku_id, fixed_offer_amount, max_offer_amount, create_time
        ))

        # 每10万条打印进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条活动SKU数据")

    return data

# 生成优惠发送记录数据（每天33,334条，30天超过100万）
def generate_offer_send_records(num_rows, activity_ids, product_ids, sku_ids, cs_ids, target_date, start_id=1):
    data = []
    customer_pool = [i for i in range(1, 10000000)]  # 1000万客户池

    for i in range(start_id, start_id + num_rows):
        record_id = i
        activity_id = random.choice(activity_ids)
        product_id = random.choice(product_ids)
        sku_id = random.choice(sku_ids)
        customer_id = random.choice(customer_pool)
        cs_id = random.choice(cs_ids)
        offer_amount = round(random.uniform(5, 500), 2)
        valid_hours = random.choices([24, 48, 72, 168], [0.4, 0.3, 0.2, 0.1])[0]  # 有效期

        # 发送时间在当天内随机
        seconds_in_day = 86400
        random_seconds = random.randint(0, seconds_in_day - 1)
        send_time = target_date + timedelta(seconds=random_seconds)
        expire_time = send_time + timedelta(hours=valid_hours)

        status = random.choices(STATUS_VALUES, weights=[0.05, 0.75, 0.2])[0]  # 状态分布
        remark = fake.sentence() if random.random() > 0.7 else None  # 30%有备注

        data.append((
            record_id, activity_id, product_id, sku_id, customer_id, cs_id,
            offer_amount, valid_hours, send_time, expire_time, status, remark
        ))

        # 每10万条打印进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条优惠发送记录数据")

    return data

# 生成优惠核销记录数据（每天33,334条，30天超过100万）
def generate_offer_redemption(num_rows, send_records, target_date, start_redemption_id=1):
    data = []
    redemption_id = start_redemption_id

    # 筛选可核销的记录（状态有效且未过期）
    valid_records = [r for r in send_records if r[10] == 1 and r[9] > target_date]

    if not valid_records:
        return data

    for i in range(num_rows):
        if not valid_records:  # 如果有效记录用完则退出
            break

        record = random.choice(valid_records)
        record_id = record[0]
        order_id = random.randint(1, 10000000)

        # 核销时间在发送时间和过期时间之间
        send_time = record[8]
        expire_time = record[9]
        redemption_time = fake.date_time_between(start_date=send_time,
                                                 end_date=min(expire_time, target_date + timedelta(days=1)))

        actual_offer_amount = record[6]  # 使用发送时的优惠金额
        payment_amount = round(random.uniform(50, 5000), 2)  # 实际支付金额

        data.append((
            redemption_id, record_id, order_id, redemption_time,
            actual_offer_amount, payment_amount
        ))
        redemption_id += 1

        # 每10万条打印进度
        if redemption_id % 100000 == 0:
            print(f"已生成 {redemption_id} 条优惠核销记录数据")

    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()
        total_days = 30
        # 日期范围：2025-07-06 至 2025-08-04（往前30天）
        end_date = datetime(2025, 8, 4)
        start_date = end_date - timedelta(days=29)  # 往前29天，共30天

        print(f"日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")

        # 1. 生成维度表数据（每表超过100万条）
        print("\n开始生成客服信息数据（目标: 1,000,000条）...")
        cs_num = 1000000  # 100万客服
        cs_data = generate_cs_info_data(cs_num)
        batch_insert(conn, 'customer_service_info',
                     ['cs_id', 'cs_name', 'department', 'position', 'hire_date', 'status'],
                     cs_data, batch_size=5000)
        cs_ids = [d[0] for d in cs_data]

        print("\n开始生成优惠活动数据（目标: 1,000,000条）...")
        activity_num = 1000000  # 100万活动
        activity_data = generate_special_offer_activity(activity_num)
        batch_insert(conn, 'special_offer_activity',
                     ['activity_id', 'activity_name', 'activity_level', 'offer_type',
                      'start_time', 'end_time', 'max_custom_amount', 'status',
                      'shop_id', 'create_time', 'update_time'],
                     activity_data, batch_size=5000)
        activity_ids = [d[0] for d in activity_data]

        print("\n开始生成活动商品数据（目标: 1,000,000条）...")
        product_num = 1000000  # 100万商品
        product_data = generate_special_offer_products(product_num, activity_ids)
        batch_insert(conn, 'special_offer_products',
                     ['activity_id', 'product_id', 'fixed_offer_amount',
                      'max_offer_amount', 'purchase_limit', 'status', 'create_time'],
                     product_data, batch_size=5000)
        product_ids = [d[1] for d in product_data]  # 商品ID在第二列

        print("\n开始生成活动SKU数据（目标: 1,000,000条）...")
        sku_num = 1000000  # 100万SKU
        sku_data = generate_special_offer_skus(sku_num, product_ids)
        batch_insert(conn, 'special_offer_skus',
                     ['product_id', 'sku_id', 'fixed_offer_amount',
                      'max_offer_amount', 'create_time'],
                     sku_data, batch_size=5000)
        sku_ids = [d[1] for d in sku_data]  # sku_id在第二列

        # 2. 按天生成事实表数据（30天，每表超过100万条）
        total_send_records = 0
        total_redemption_records = 0
        next_record_id = 1  # 全局记录ID
        next_redemption_id = 1  # 全局核销ID

        for day in range(total_days):
            current_date = start_date + timedelta(days=day)
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n===== 开始生成 {date_str} 的事实表数据 =====")

            # 优惠发送记录 - 每天生成33,334条（30天总计1,000,020条）
            print(f"生成优惠发送记录数据 ({date_str})...")
            daily_send_records = 33334
            send_data = generate_offer_send_records(
                daily_send_records, activity_ids, product_ids, sku_ids, cs_ids, current_date, next_record_id
            )
            batch_insert(conn, 'offer_send_records',
                         ['record_id', 'activity_id', 'product_id', 'sku_id', 'customer_id', 'cs_id',
                          'offer_amount', 'valid_hours', 'send_time', 'expire_time', 'status', 'remark'],
                         send_data, batch_size=5000)
            next_record_id += daily_send_records
            total_send_records += daily_send_records

            # 优惠核销记录 - 每天生成33,334条（30天总计1,000,020条）
            print(f"生成优惠核销记录数据 ({date_str})...")
            daily_redemption_records = 33334
            redemption_data = generate_offer_redemption(
                daily_redemption_records, send_data, current_date, next_redemption_id
            )
            if redemption_data:
                batch_insert(conn, 'offer_redemption_records',
                             ['redemption_id', 'record_id', 'order_id', 'redemption_time',
                              'actual_offer_amount', 'payment_amount'],
                             redemption_data, batch_size=5000)
                next_redemption_id += len(redemption_data)
                total_redemption_records += len(redemption_data)

            print(f"===== {date_str} 数据处理完成 =====")

        print(f"\n所有数据处理完成！生成日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        print(f"总数据量：客服信息({cs_num}), 优惠活动({activity_num}), 活动商品({product_num}), 活动SKU({sku_num})")
        print(f"        发送记录({total_send_records}), 核销记录({total_redemption_records})")
        print(f"        总计：{cs_num + activity_num + product_num + sku_num + total_send_records + total_redemption_records}条记录")

    except Exception as e:
        print(f"发生错误: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n总耗时: {int(total_time//60)}分{total_time%60:.2f}秒")

if __name__ == "__main__":
    main()