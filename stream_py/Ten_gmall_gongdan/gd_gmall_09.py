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

# 表结构配置（移除dt相关配置）
PAGE_TYPES = ['shop', 'product', 'other']
DEVICE_TYPES = ['mobile', 'pc', 'applet']
OS_TYPES = ['iOS', 'Android', 'Windows', 'macOS']
BROWSER_TYPES = ['Chrome', 'Safari', 'Firefox', 'Edge', '微信浏览器']
PROVINCES = ['北京', '上海', '广东', '江苏', '浙江', '山东', '河南', '四川']
CITIES = {
    '北京': ['北京市'],
    '上海': ['上海市'],
    '广东': ['广州市', '深圳市', '东莞市'],
    '江苏': ['南京市', '苏州市', '无锡市'],
    '浙江': ['杭州市', '宁波市', '温州市']
}
SHOP_PAGE_TYPES = ['home', 'activity', 'category', 'new']
SOURCE_TYPES = ['search', 'social', 'ad', 'direct']
ACTIVITY_TYPES = ['满减', '折扣', '秒杀', '赠品']

# 创建数据库连接
def create_connection():
    try:
        conn = pymysql.connect(**db_config)
        print("数据库连接成功")
        return conn
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        raise

# 批量插入函数（移除dt相关处理）
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

# 生成商品信息数据（移除dt）
def generate_product_info_data(num_rows, conn):
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE product_info")
        conn.commit()

    data = []
    for i in range(1, num_rows + 1):
        product_id = i
        product_name = f"{fake.word()}{fake.random_letter().upper()}{i}商品"
        category_id = random.randint(1, 50)
        category_name = fake.word() + '类'
        brand_id = random.randint(1, 20)
        brand_name = fake.word() + '品牌'
        price = round(random.uniform(10, 2000), 2)
        cost_price = round(price * random.uniform(0.5, 0.8), 2)
        status = 1 if random.random() > 0.1 else 0
        create_time = fake.date_time_between(start_date='-2y', end_date='-30d')
        data.append((
            product_id, product_name, category_id, category_name, brand_id,
            brand_name, price, cost_price, status, create_time
        ))
    return data

# 生成店铺页面信息数据（移除dt）
def generate_shop_page_info_data(num_rows, conn):
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE shop_page_info")
        conn.commit()

    data = []
    for i in range(1, num_rows + 1):
        page_id = i
        page_name = f"{fake.word()}{random.choice(['首页', '活动页', '分类页', '新品页'])}"
        page_url = f"https://example.com/shop/{page_id}.html"
        page_type = random.choice(SHOP_PAGE_TYPES)
        page_level = random.randint(1, 3)
        parent_page_id = random.randint(0, i-1) if i > 1 else 0
        is_active = 1 if random.random() > 0.1 else 0
        create_time = fake.date_time_between(start_date='-1y', end_date='-1d')
        data.append((
            page_id, page_name, page_url, page_type, page_level,
            parent_page_id, is_active, create_time
        ))
    return data

# 生成用户信息数据（移除dt）
def generate_user_info_data(num_rows, conn):
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE user_info")
        conn.commit()

    data = []
    for i in range(1, num_rows + 1):
        user_id = i
        user_name = fake.user_name()
        gender = random.choice(['男', '女'])
        age = random.randint(18, 60)
        register_time = fake.date_time_between(start_date='-3y', end_date='-1d')
        vip_level = random.randint(0, 5)
        province = random.choice(PROVINCES)
        city = random.choice(CITIES.get(province, [province]))
        phone_prefix = random.choice(['138', '139', '135', '136', '186', '177'])
        data.append((
            user_id, user_name, gender, age, register_time, vip_level,
            province, city, phone_prefix
        ))
    return data

# 生成页面访问日志数据（移除dt）
def generate_page_visit_log_data(num_rows, product_ids, shop_page_ids, user_ids, target_date):
    data = []
    session_pool = [f"session_{i}" for i in range(1, 100000)]
    user_pool = user_ids

    for i in range(1, num_rows + 1):
        log_id = i
        user_id = random.choice(user_pool)
        session_id = random.choice(session_pool)
        page_type = random.choices(PAGE_TYPES, weights=[0.3, 0.5, 0.2], k=1)[0]

        if page_type == 'product':
            page_id = random.choice(product_ids)
            page_url = f"https://example.com/product/{page_id}.html"
        else:
            page_id = random.choice(shop_page_ids)
            page_url = f"https://example.com/shop/{page_id}.html"

        referer_url = fake.url() if random.random() > 0.3 else None
        referer_page_id = random.choice(shop_page_ids + product_ids) if referer_url else None

        seconds_in_day = 86400
        random_seconds = random.randint(0, seconds_in_day - 1)
        visit_time = target_date + timedelta(seconds=random_seconds)

        stay_duration = random.randint(5, 300)
        device_type = random.choice(DEVICE_TYPES)
        os_type = random.choice(OS_TYPES)
        browser_type = random.choice(BROWSER_TYPES)
        ip_address = fake.ipv4()
        province = random.choice(PROVINCES)
        city = random.choice(CITIES.get(province, [province]))
        is_new_visitor = 1 if random.random() < 0.2 else 0

        data.append((
            log_id, user_id, session_id, page_url, page_type, page_id,
            referer_url, referer_page_id, visit_time, stay_duration,
            device_type, os_type, browser_type, ip_address, province,
            city, is_new_visitor
        ))

        if i % 100000 == 0:
            print(f"已生成 {i} 条页面访问日志数据")

    return data

# 生成订单信息数据（移除dt）
def generate_order_info_data(num_rows, user_ids, product_ids, target_date):
    data = []
    for i in range(1, num_rows + 1):
        order_id = i
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        order_amount = round(random.uniform(50, 5000), 2)
        payment_amount = round(order_amount * random.uniform(0.8, 1.0), 2)
        order_time = fake.date_time_between(start_date=target_date, end_date=target_date + timedelta(days=1))
        payment_time = order_time + timedelta(minutes=random.randint(1, 120)) if random.random() > 0.2 else None
        order_status = 1 if payment_time else 0
        data.append((
            order_id, user_id, product_id, order_amount, payment_amount,
            order_time, payment_time, order_status
        ))
    return data

# 生成流量来源数据（移除dt）
def generate_traffic_source_data(num_rows, target_date):
    data = []
    for i in range(1, num_rows + 1):
        source_id = i
        source_type = random.choice(SOURCE_TYPES)
        source_name = fake.word() + random.choice(['搜索', '社交', '广告'])
        source_url = fake.url() if source_type != 'direct' else None
        campaign_id = random.randint(1, 100) if random.random() > 0.5 else None
        campaign_name = f"{fake.word()}活动" if campaign_id else None
        data.append((
            source_id, source_type, source_name, source_url, campaign_id,
            campaign_name
        ))
    return data

# 生成营销活动数据（移除dt）
def generate_marketing_activity_data(num_rows, target_date):
    data = []
    for i in range(1, num_rows + 1):
        activity_id = i
        activity_name = random.choice(ACTIVITY_TYPES) + f"活动_{i}"
        start_time = fake.date_time_between(start_date=target_date - timedelta(days=7), end_date=target_date)
        end_time = start_time + timedelta(days=random.randint(1, 14))
        activity_type = random.choice(ACTIVITY_TYPES)
        data.append((
            activity_id, activity_name, start_time, end_time, activity_type
        ))
    return data

# 生成页面跳转关系数据（移除dt）
def generate_page_relationship_data(num_rows, all_page_ids, target_date):
    data = []
    for i in range(1, num_rows + 1):
        from_page_id = random.choice(all_page_ids)
        to_page_id = random.choice(all_page_ids)
        while from_page_id == to_page_id:
            to_page_id = random.choice(all_page_ids)
        relation_type = random.choice(['click', 'redirect', 'auto'])
        create_time = fake.date_time_between(start_date=target_date - timedelta(days=1), end_date=target_date)
        data.append((
            from_page_id, to_page_id, relation_type, create_time
        ))
    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()
        total_days = 30
        # 日期范围：2025-07-02 至 2025-07-31（共30天）
        start_date = datetime(2025, 7, 2)
        end_date = datetime(2025, 7, 31)

        # 1. 生成维度表数据（无dt字段）
        print("开始生成商品信息数据...")
        product_num = 100000
        product_data = generate_product_info_data(product_num, conn)
        batch_insert(conn, 'product_info',
                     ['product_id', 'product_name', 'category_id', 'category_name', 'brand_id',
                      'brand_name', 'price', 'cost_price', 'status', 'create_time'],
                     product_data)
        product_ids = [i for i in range(1, product_num + 1)]

        print("\n开始生成店铺页面信息数据...")
        shop_page_num = 50000
        shop_page_data = generate_shop_page_info_data(shop_page_num, conn)
        batch_insert(conn, 'shop_page_info',
                     ['page_id', 'page_name', 'page_url', 'page_type', 'page_level',
                      'parent_page_id', 'is_active', 'create_time'],
                     shop_page_data)
        shop_page_ids = [i for i in range(1, shop_page_num + 1)]
        all_page_ids = product_ids + shop_page_ids

        print("\n开始生成用户信息数据...")
        user_num = 200000
        user_data = generate_user_info_data(user_num, conn)
        batch_insert(conn, 'user_info',
                     ['user_id', 'user_name', 'gender', 'age', 'register_time', 'vip_level',
                      'province', 'city', 'phone_prefix'],
                     user_data)
        user_ids = [i for i in range(1, user_num + 1)]

        print("\n开始生成营销活动数据...")
        activity_num = 1000
        activity_data = generate_marketing_activity_data(activity_num, end_date)  # 基于结束日期生成
        batch_insert(conn, 'marketing_activity',
                     ['activity_id', 'activity_name', 'start_time', 'end_time', 'activity_type'],
                     activity_data)

        # 2. 按天生成事实表数据（30天，至2025-07-31结束）
        for day in range(total_days):
            current_date = start_date + timedelta(days=day)
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n===== 开始生成 {date_str} 的事实表数据 =====")

            # 页面访问日志
            print(f"生成页面访问日志数据 ({date_str})...")
            visit_data = generate_page_visit_log_data(120000, product_ids, shop_page_ids, user_ids, current_date)
            batch_insert(conn, 'page_visit_log',
                         ['log_id', 'user_id', 'session_id', 'page_url', 'page_type', 'page_id',
                          'referer_url', 'referer_page_id', 'visit_time', 'stay_duration',
                          'device_type', 'os_type', 'browser_type', 'ip_address', 'province',
                          'city', 'is_new_visitor'],
                         visit_data)

            # 订单信息
            print(f"生成订单信息数据 ({date_str})...")
            order_data = generate_order_info_data(10000, user_ids, product_ids, current_date)
            batch_insert(conn, 'order_info',
                         ['order_id', 'user_id', 'product_id', 'order_amount', 'payment_amount',
                          'order_time', 'payment_time', 'order_status'],
                         order_data)

            # 流量来源
            print(f"生成流量来源数据 ({date_str})...")
            source_data = generate_traffic_source_data(5000, current_date)
            batch_insert(conn, 'traffic_source',
                         ['source_id', 'source_type', 'source_name', 'source_url', 'campaign_id',
                          'campaign_name'],
                         source_data)

            # 页面跳转关系
            print(f"生成页面跳转关系数据 ({date_str})...")
            relation_data = generate_page_relationship_data(30000, all_page_ids, current_date)
            batch_insert(conn, 'page_relationship',
                         ['from_page_id', 'to_page_id', 'relation_type', 'create_time'],
                         relation_data)

            print(f"===== {date_str} 数据处理完成 =====")

        print(f"\n所有数据处理完成！生成日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}（共30天）")

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