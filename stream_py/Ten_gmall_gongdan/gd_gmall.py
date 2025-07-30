import pymysql
from faker import Faker
import random
import time
from datetime import datetime, timedelta

# 初始化Faker和随机种子，确保可重复性
fake = Faker('zh_CN')
Faker.seed(42)
random.seed(42)

# 数据库配置
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': 'root',
    'database': 'gd_gmall',
    'charset': 'utf8mb4'
}

# 表结构配置
PAGE_TYPES = ['shop_page', 'store_item', 'store_other']
DEVICE_TYPES = ['wireless', 'pc']
PAGE_SECTIONS = ['header', 'content', 'sidebar', 'footer']
SHOP_PAGE_TYPES = ['home', 'activity', 'category', 'new']
SOURCE_TYPES = ['direct', 'search', 'social']

# 创建数据库连接
def create_connection():
    try:
        conn = pymysql.connect(**db_config)
        print("数据库连接成功")
        return conn
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        raise

# 批量插入函数，增加处理重复数据的逻辑，这里使用 INSERT IGNORE
def batch_insert(conn, table_name, columns, data, batch_size=1000):
    if not data:
        print(f"没有数据插入到 {table_name}")
        return

    with conn.cursor() as cursor:
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        # 使用 INSERT IGNORE 忽略重复主键的插入
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

# 生成商品数据（一次性生成），如果是全量生成，可先清空表
def generate_product_data(num_rows, conn):
    # 清空 product 表数据
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE product")
        conn.commit()

    data = []
    for i in range(1, num_rows + 1):
        page_id = f"product_{i}"
        product_id = f"prod_{i}"
        product_name = f"{fake.word()}{fake.random_letter().upper()}{i}"
        category = fake.word()
        page_section = random.choice(PAGE_SECTIONS)
        create_time = fake.date_time_between(start_date='-2y', end_date='-30d')
        data.append((
            page_id, product_id, product_name, category,
            page_section, create_time
        ))
    return data

# 生成店铺数据（一次性生成），如果是全量生成，可先清空表
def generate_shop_data(num_rows, conn):
    # 清空 shop 表数据
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE shop")
        conn.commit()

    data = []
    for i in range(1, num_rows + 1):
        page_id = f"shop_{i}"
        page_name = f"{fake.word()} {fake.random_element(elements=('首页', '活动页', '分类页', '新品页'))}"
        page_type = random.choice(SHOP_PAGE_TYPES)
        template_id = f"template_{random.randint(1, 20)}"
        data.append((page_id, page_name, page_type, template_id))
    return data

# 生成用户日志数据（按天生成）
def generate_user_log_data(num_rows, product_pages, shop_pages, target_date):
    # 为2025-07-30生成更多样化的数据
    if target_date.strftime('%Y-%m-%d') == "2025-07-30":
        session_pool = [f"session_{i}" for i in range(1, 1000001)]  # 更大的会话池
        user_pool = [i for i in range(1, 200001)]  # 更多的用户
        print(f"为 2025-07-30 创建更大的用户池和会话池")
    else:
        session_pool = [f"session_{i}" for i in range(1, 500001)]
        user_pool = [i for i in range(1, 100001)]

    data = []

    for i in range(1, num_rows + 1):
        session_id = random.choice(session_pool)
        user_id = random.choice(user_pool)
        device_type = random.choice(DEVICE_TYPES)

        # 为2025-07-30增加更多商品页面的权重
        if target_date.strftime('%Y-%m-%d') == "2025-07-30":
            weights = [0.2, 0.7, 0.1]  # 商品页占70%
            order_prob = 0.2
        else:
            weights = [0.3, 0.6, 0.1]
            order_prob = 0.15

        page_type = random.choices(PAGE_TYPES, weights=weights, k=1)[0]
        if page_type == 'store_item':
            page = random.choice(product_pages)
        else:
            page = random.choice(shop_pages)
        page_id = page[0]

        refer_page_id = None
        if random.random() < 0.3 and data:
            prev_page = random.choice(data[-100:]) if len(data) > 100 else random.choice(data)
            refer_page_id = prev_page[3]

        seconds_in_day = 86400
        random_seconds = random.randint(0, seconds_in_day - 1)
        visit_time = target_date + timedelta(seconds=random_seconds)

        stay_duration = random.randint(5, 300)
        # 根据页面类型调整下单概率
        if page_type == 'store_item':
            is_order = 1 if random.random() < order_prob else 0
        else:
            is_order = 1 if random.random() < (order_prob * 0.4) else 0

        data.append((
            i, session_id, user_id, page_id, page_type,
            refer_page_id, device_type, visit_time,
            stay_duration, is_order
        ))

        if i % 100000 == 0:
            print(f"已生成 {i} 条用户日志数据")

    return data

# 生成店内路径数据（按天生成）
def generate_store_path_data(num_rows, product_pages, shop_pages):
    all_pages = product_pages + shop_pages
    data = []

    for i in range(1, num_rows + 1):
        from_page = random.choice(all_pages)
        to_page = random.choice(all_pages)
        while from_page[0] == to_page[0]:
            to_page = random.choice(all_pages)

        path_count = random.randint(1, 1000)
        avg_stay_duration = round(random.uniform(5, 180), 2)
        conversion_rate = round(random.uniform(0, 0.2), 4)
        device_type = random.choice(DEVICE_TYPES)

        data.append((
            from_page[0], to_page[0], path_count,
            avg_stay_duration, conversion_rate, device_type
        ))

        if i % 100000 == 0:
            print(f"已生成 {i} 条店内路径数据")

    return data

# 生成流量来源数据（按天生成）
def generate_traffic_source_data(num_rows):
    source_pages = [f"source_{i}" for i in range(1, 1001)]
    source_pages.extend([None] * 100)  # 直接访问
    data = []

    for i in range(1, num_rows + 1):
        source_page_id = random.choice(source_pages)
        source_type = random.choice(SOURCE_TYPES)
        session_count = random.randint(1, 5000)
        avg_session_duration = round(random.uniform(10, 600), 2)

        data.append((
            source_page_id, source_type, session_count,
            avg_session_duration
        ))

        if i % 100000 == 0:
            print(f"已生成 {i} 条流量来源数据")

    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()

        # 1. 一次性生成维度表数据
        print("开始生成商品数据...")
        product_data = generate_product_data(1000000, conn)
        batch_insert(conn, 'product',
                     ['page_id', 'product_id', 'product_name', 'category', 'page_section', 'create_time'],
                     product_data)

        print("\n开始生成店铺数据...")
        shop_data = generate_shop_data(1000000, conn)
        batch_insert(conn, 'shop',
                     ['page_id', 'page_name', 'page_type', 'template_id'],
                     shop_data)

        # 2. 按天生成事实表数据（30天）
        start_date = datetime(2025, 7, 1)
        total_days = 30
        daily_rows = 120000  # 每天12万条

        # 为2025-07-30生成更多数据
        target_date_str = "2025-07-30"
        total_user_logs = 0
        total_store_paths = 0
        total_traffic_sources = 0

        for day in range(total_days):
            current_date = start_date + timedelta(days=day)
            date_str = current_date.strftime('%Y-%m-%d')

            # 为2025-07-30生成更多数据
            if date_str == target_date_str:
                daily_rows_target = 200000  # 20万条数据
                print(f"\n===== 开始生成目标日期 {date_str} 的数据 (额外数据量) =====")
            else:
                daily_rows_target = daily_rows
                print(f"\n===== 开始生成 {date_str} 的数据 =====")

            # 用户日志数据
            print(f"生成用户日志数据 ({date_str})...")
            user_log_data = generate_user_log_data(daily_rows_target, product_data, shop_data, current_date)
            batch_insert(conn, 'user_log',
                         ['log_id', 'session_id', 'user_id', 'page_id', 'page_type',
                          'refer_page_id', 'device_type', 'visit_time', 'stay_duration',
                          'is_order'],
                         user_log_data)
            total_user_logs += len(user_log_data)

            # 店内路径数据
            print(f"生成店内路径数据 ({date_str})...")
            store_path_data = generate_store_path_data(daily_rows_target, product_data, shop_data)
            batch_insert(conn, 'store_path',
                         ['from_page_id', 'to_page_id', 'path_count',
                          'avg_stay_duration', 'conversion_rate', 'device_type'],
                         store_path_data)
            total_store_paths += len(store_path_data)

            # 流量来源数据
            print(f"生成流量来源数据 ({date_str})...")
            traffic_source_data = generate_traffic_source_data(daily_rows_target)
            batch_insert(conn, 'traffic_source',
                         ['source_page_id', 'source_type', 'session_count',
                          'avg_session_duration'],
                         traffic_source_data)
            total_traffic_sources += len(traffic_source_data)

            print(f"===== {date_str} 数据处理完成 =====")

        print("\n所有日期数据处理完成!")
        print(f"用户日志总数据量: {total_user_logs} 条")
        print(f"店内路径总数据量: {total_store_paths} 条")
        print(f"流量来源总数据量: {total_traffic_sources} 条")

    except Exception as e:
        print(f"发生错误: {e}")
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