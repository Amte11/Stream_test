import pymysql
from faker import Faker
import random
import time
from datetime import datetime, timedelta

# 初始化Faker和随机种子，确保可重复性
fake = Faker('zh_CN')
Faker.seed(42)  # 使用固定种子，确保数据可重复生成
random.seed(42)


# 数据库配置 - 请替换为你的实际数据库信息
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': 'root',
    'database': 'gd_gmall',
    'charset': 'utf8mb4'
}

# 固定日期：2025-07-30
TARGET_DATE = datetime(2025, 7, 30)
TARGET_DATE_STR = TARGET_DATE.date()

# 表结构配置
PAGE_TYPES = ['shop_page', 'store_item', 'store_other']
DEVICE_TYPES = ['wireless', 'pc']
PAGE_SECTIONS = ['header', 'content', 'sidebar', 'footer']
SHOP_PAGE_TYPES = ['home', 'activity', 'category', 'new']
SOURCE_TYPES = ['direct', 'search', 'social']

# 创建数据库连接
def create_connection():
    """创建并返回数据库连接"""
    try:
        conn = pymysql.connect(**db_config)
        print("数据库连接成功")
        return conn
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        raise

# 批量插入函数
def batch_insert(conn, table_name, columns, data, batch_size=1000):
    """批量插入数据到指定表"""
    if not data:
        print(f"没有数据插入到 {table_name}")
        return

    with conn.cursor() as cursor:
        # 准备插入语句
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # 分批插入
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

# 生成商品数据
def generate_product_data(num_rows):
    """生成商品表数据，创建时间在目标日期之前"""
    data = []
    for i in range(1, num_rows + 1):
        page_id = f"product_{i}"
        product_id = f"prod_{i}"
        product_name = f"{fake.word()}{fake.random_letter().upper()}{i}"
        category = fake.word()
        page_section = random.choice(PAGE_SECTIONS)

        # 确保商品创建时间在目标日期之前
        create_time = fake.date_time_between(
            start_date='-2y',
            end_date=TARGET_DATE
        )

        data.append((
            page_id, product_id, product_name, category,
            page_section, create_time
        ))
    return data

# 生成店铺数据
def generate_shop_data(num_rows):
    """生成店铺表数据"""
    data = []
    for i in range(1, num_rows + 1):
        page_id = f"shop_{i}"
        page_name = f"{fake.word()} {fake.random_element(elements=('首页', '活动页', '分类页', '新品页'))}"
        page_type = random.choice(SHOP_PAGE_TYPES)
        template_id = f"template_{random.randint(1, 20)}"

        data.append((page_id, page_name, page_type, template_id))
    return data

# 生成用户日志数据
def generate_user_log_data(num_rows, product_pages, shop_pages):
    """生成用户行为日志数据，所有记录日期为2025-07-30"""
    # 创建会话ID池和用户ID池
    session_pool = [f"session_{i}" for i in range(1, 500001)]
    user_pool = [i for i in range(1, 100001)]

    data = []

    for i in range(1, num_rows + 1):
        session_id = random.choice(session_pool)
        user_id = random.choice(user_pool)
        device_type = random.choice(DEVICE_TYPES)

        # 随机选择页面类型（带权重）
        page_type = random.choices(
            PAGE_TYPES,
            weights=[0.3, 0.6, 0.1],  # 商品页占60%，店铺页30%，其他10%
            k=1
        )[0]

        # 根据页面类型选择具体页面
        if page_type == 'store_item':
            page = random.choice(product_pages)
        else:
            page = random.choice(shop_pages)

        page_id = page[0]  # 获取页面ID

        # 来源页面（约30%的记录有来源）
        refer_page_id = None
        if random.random() < 0.3 and data:  # 确保data不为空
            # 从最近100条记录中随机选择一个来源页面
            prev_page = random.choice(data[-100:]) if len(data) > 100 else random.choice(data)
            refer_page_id = prev_page[3]  # 前一条记录的page_id

        # 访问时间（在2025-07-30这一天内随机）
        # 生成当天00:00:00到23:59:59之间的随机时间
        seconds_in_day = 86400  # 24*60*60
        random_seconds = random.randint(0, seconds_in_day - 1)
        visit_time = TARGET_DATE + timedelta(seconds=random_seconds)

        # 停留时长（5-300秒）
        stay_duration = random.randint(5, 300)

        # 是否下单（商品页下单概率15%，其他页5%）
        is_order = 1 if (random.random() < 0.15 if page_type == 'store_item' else random.random() < 0.05) else 0

        data.append((
            i, session_id, user_id, page_id, page_type,
            refer_page_id, device_type, visit_time,
            stay_duration, is_order, TARGET_DATE_STR
        ))

        # 每10万条打印一次进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条用户日志数据")

    return data

# 生成店内路径数据
def generate_store_path_data(num_rows, product_pages, shop_pages):
    """生成店内路径数据，所有记录日期为2025-07-30"""
    data = []

    # 合并商品和店铺页面
    all_pages = product_pages + shop_pages

    for i in range(1, num_rows + 1):
        # 随机选择来源页面和去向页面（排除自身跳转）
        from_page = random.choice(all_pages)
        to_page = random.choice(all_pages)
        while from_page[0] == to_page[0]:  # 避免自己到自己的路径
            to_page = random.choice(all_pages)

        # 路径数量（1-1000）
        path_count = random.randint(1, 1000)

        # 平均停留时长（5-180秒）
        avg_stay_duration = round(random.uniform(5, 180), 2)

        # 转化率（0-1）
        conversion_rate = round(random.uniform(0, 0.2), 4)

        # 设备类型
        device_type = random.choice(DEVICE_TYPES)

        data.append((
            from_page[0], to_page[0], path_count,
            avg_stay_duration, conversion_rate,
            TARGET_DATE_STR, device_type
        ))

        # 每10万条打印一次进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条店内路径数据")

    return data

# 生成流量来源数据
def generate_traffic_source_data(num_rows):
    """生成流量来源数据，所有记录日期为2025-07-30"""
    data = []

    # 创建来源页面池（包含空值表示直接访问）
    source_pages = [f"source_{i}" for i in range(1, 1001)]
    source_pages.extend([None] * 100)  # 10%的概率是直接访问（无来源页面）

    for i in range(1, num_rows + 1):
        source_page_id = random.choice(source_pages)
        source_type = random.choice(SOURCE_TYPES)

        # 会话数（1-5000）
        session_count = random.randint(1, 5000)

        # 平均会话时长（10-600秒）
        avg_session_duration = round(random.uniform(10, 600), 2)

        data.append((
            source_page_id, source_type, session_count,
            avg_session_duration, TARGET_DATE_STR
        ))

        # 每10万条打印一次进度
        if i % 100000 == 0:
            print(f"已生成 {i} 条流量来源数据")

    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()

        # 1. 生成并插入商品数据
        print("开始生成商品数据...")
        product_data = generate_product_data(1000000)
        batch_insert(conn, 'product',
                     ['page_id', 'product_id', 'product_name', 'category', 'page_section', 'create_time'],
                     product_data)

        # 2. 生成并插入店铺数据
        print("\n开始生成店铺数据...")
        shop_data = generate_shop_data(1000000)
        batch_insert(conn, 'shop',
                     ['page_id', 'page_name', 'page_type', 'template_id'],
                     shop_data)

        # 3. 生成并插入用户日志数据
        print("\n开始生成用户日志数据...")
        user_log_data = generate_user_log_data(1000000, product_data, shop_data)
        batch_insert(conn, 'user_log',
                     ['log_id', 'session_id', 'user_id', 'page_id', 'page_type',
                      'refer_page_id', 'device_type', 'visit_time', 'stay_duration',
                      'is_order', 'dt'],
                     user_log_data)

        # 4. 生成并插入店内路径数据
        print("\n开始生成店内路径数据...")
        store_path_data = generate_store_path_data(1000000, product_data, shop_data)
        batch_insert(conn, 'store_path',
                     ['from_page_id', 'to_page_id', 'path_count',
                      'avg_stay_duration', 'conversion_rate', 'dt', 'device_type'],
                     store_path_data)

        # 5. 生成并插入流量来源数据
        print("\n开始生成流量来源数据...")
        traffic_source_data = generate_traffic_source_data(1000000)
        batch_insert(conn, 'traffic_source',
                     ['source_page_id', 'source_type', 'session_count',
                      'avg_session_duration', 'dt'],
                     traffic_source_data)

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
        print(f"\n所有数据处理完成! 总耗时: {int(total_time//60)}分{total_time%60:.2f}秒")

if __name__ == "__main__":
    main()
