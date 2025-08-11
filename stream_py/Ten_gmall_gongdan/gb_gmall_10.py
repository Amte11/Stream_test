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
    'database': 'gb_gmall_10',
    'charset': 'utf8mb4'
}

# 页面类型和模块类型
PAGE_TYPES = ['home', 'custom', 'product_detail']
MODULE_TYPES = ['推荐型', '活动型', '导航型', '促销型', '商品展示型']
DATA_VERSIONS = ['v1.0', 'v1.1', 'v1.2', 'v2.0', 'v2.1']

# 时间范围配置（仅生成近30天数据）
END_DATE_STR = '2025-08-05'  # 结束日期
DAYS_BEFORE = 30  # 往前推30天

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

# 生成页面ID池（50万个唯一页面）
def generate_page_ids(num_pages):
    return [f"page_{str(i).zfill(8)}" for i in range(1, num_pages + 1)]

# 生成随机时间（仅近30天）
def generate_random_time():
    """生成结束日期前30天内的随机时间"""
    end_date = datetime.strptime(END_DATE_STR, '%Y-%m-%d')
    start_date = end_date - timedelta(days=DAYS_BEFORE)
    # 计算时间差（秒）
    time_delta = (end_date - start_date).total_seconds()
    # 随机生成时间差
    random_seconds = random.uniform(0, time_delta)
    # 计算随机时间
    random_time = start_date + timedelta(seconds=random_seconds)
    return random_time

# 生成随机日期（仅近30天，仅日期部分）
def generate_random_date():
    """生成结束日期前30天内的随机日期"""
    end_date = datetime.strptime(END_DATE_STR, '%Y-%m-%d')
    start_date = end_date - timedelta(days=DAYS_BEFORE)
    # 计算天数差
    day_delta = (end_date - start_date).days
    # 随机生成天数
    random_days = random.randint(0, day_delta)
    # 计算随机日期
    random_date = start_date + timedelta(days=random_days)
    return random_date

# 生成页面基础数据（超过100万条）
def generate_page_visit_base(num_rows, page_ids):
    data = []
    page_names = ["首页", "618活动承接页", "双11主会场", "商品详情页", "会员中心",
                  "新人专享页", "品牌特卖页", "限时秒杀页", "购物车页", "分类页"]

    for i in range(num_rows):
        page_id = random.choice(page_ids)
        page_name = random.choice(page_names)
        page_type = random.choice(PAGE_TYPES)

        # 生成访问时间（近30天）
        visit_time = generate_random_time()

        # 生成访问数据
        visitor_count = random.randint(100, 10000)
        visit_pv = visitor_count * random.randint(1, 5)
        click_pv = int(visit_pv * random.uniform(0.2, 0.8))

        data.append((
            page_id, page_name, page_type, visit_time,
            visitor_count, visit_pv, click_pv, "business_analyzer"
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条页面访问基础数据")

    return data

# 生成页面板块点击数据（超过100万条）
def generate_page_block_click(num_rows, page_ids):
    data = []
    block_names = ["首页轮播区", "商品推荐区", "促销活动区", "品牌展示区", "新品上市区",
                   "限时秒杀区", "会员专享区", "分类导航区", "底部推荐区", "侧边栏广告区"]

    for i in range(num_rows):
        page_id = random.choice(page_ids)
        block_id = f"block_{str(random.randint(1, 10000)).zfill(5)}"
        block_name = random.choice(block_names)

        # 生成点击数据
        click_pv = random.randint(100, 10000)
        click_uv = int(click_pv * random.uniform(0.3, 0.7))
        guide_pay_amount = round(click_pv * random.uniform(1.5, 10.0), 2)

        # 采集时间（近30天）
        collect_time = generate_random_time()

        data.append((
            page_id, block_id, block_name, click_pv, click_uv, guide_pay_amount, collect_time
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条板块点击数据")

    return data

# 生成页面数据趋势（近30天数据）
def generate_page_data_trend(num_rows, page_ids):
    data = []

    for i in range(num_rows):
        page_id = random.choice(page_ids)

        # 统计日期（近30天）
        stat_date = generate_random_date()

        # 生成趋势数据
        visitor_count = random.randint(100, 10000)
        click_uv = int(visitor_count * random.uniform(0.4, 0.9))
        data_version = random.choice(DATA_VERSIONS)

        data.append((
            page_id, stat_date, visitor_count, click_uv, data_version
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条数据趋势数据")

    return data

# 生成页面引导商品数据（近30天）
def generate_page_guide_product(num_rows, page_ids):
    data = []

    for i in range(num_rows):
        page_id = random.choice(page_ids)
        product_id = f"prod_{str(random.randint(1, 1000000)).zfill(8)}"

        # 生成引导数据
        guide_count = random.randint(50, 5000)
        guide_buyer_count = int(guide_count * random.uniform(0.05, 0.3))

        # 引导时间（近30天）
        guide_time = generate_random_time()

        data.append((
            page_id, product_id, guide_count, guide_buyer_count, guide_time
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条引导商品数据")

    return data

# 生成页面模块明细（近30天）
def generate_page_module_detail(num_rows, page_ids):
    data = []
    module_names = ["新品推荐模块", "优惠活动模块", "热销商品模块", "品牌精选模块", "限时秒杀模块",
                    "会员专享模块", "猜你喜欢模块", "浏览历史模块", "分类导航模块", "广告推广模块"]

    for i in range(num_rows):
        page_id = random.choice(page_ids)
        module_id = f"mod_{str(random.randint(1, 10000)).zfill(5)}"
        module_name = random.choice(module_names)
        module_type = random.choice(MODULE_TYPES)

        # 生成模块数据
        expose_pv = random.randint(1000, 50000)
        interact_count = int(expose_pv * random.uniform(0.1, 0.5))

        # 统计时间（近30天）
        stat_time = generate_random_time()

        data.append((
            page_id, module_id, module_name, module_type,
            expose_pv, interact_count, stat_time
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条模块明细数据")

    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()
        num_records = 2000000  # 每张表200万条数据

        # 生成50万个唯一页面ID
        print("生成页面ID池...")
        page_ids = generate_page_ids(500000)
        print(f"已生成 {len(page_ids)} 个页面ID")
        print(f"数据时间范围：{datetime.strptime(END_DATE_STR, '%Y-%m-%d') - timedelta(days=DAYS_BEFORE)} 至 {END_DATE_STR}")

        # 1. 生成页面访问基础数据
        print("\n开始生成页面访问基础数据 (page_visit_base)...")
        visit_base_data = generate_page_visit_base(num_records, page_ids)
        batch_insert(conn, 'page_visit_base',
                     ['page_id', 'page_name', 'page_type', 'visit_time',
                      'visitor_count', 'visit_pv', 'click_pv', 'data_source'],
                     visit_base_data, batch_size=5000)

        # 2. 生成页面板块点击数据
        print("\n开始生成页面板块点击数据 (page_block_click)...")
        block_click_data = generate_page_block_click(num_records, page_ids)
        batch_insert(conn, 'page_block_click',
                     ['page_id', 'block_id', 'block_name', 'click_pv',
                      'click_uv', 'guide_pay_amount', 'collect_time'],
                     block_click_data, batch_size=5000)

        # 3. 生成页面数据趋势
        print("\n开始生成页面数据趋势 (page_data_trend)...")
        data_trend_data = generate_page_data_trend(num_records, page_ids)
        batch_insert(conn, 'page_data_trend',
                     ['page_id', 'stat_date', 'visitor_count', 'click_uv', 'data_version'],
                     data_trend_data, batch_size=5000)

        # 4. 生成页面引导商品数据
        print("\n开始生成页面引导商品数据 (page_guide_product)...")
        guide_product_data = generate_page_guide_product(num_records, page_ids)
        batch_insert(conn, 'page_guide_product',
                     ['page_id', 'product_id', 'guide_count', 'guide_buyer_count', 'guide_time'],
                     guide_product_data, batch_size=5000)

        # 5. 生成页面模块明细
        print("\n开始生成页面模块明细 (page_module_detail)...")
        module_detail_data = generate_page_module_detail(num_records, page_ids)
        batch_insert(conn, 'page_module_detail',
                     ['page_id', 'module_id', 'module_name', 'module_type',
                      'expose_pv', 'interact_count', 'stat_time'],
                     module_detail_data, batch_size=5000)

        print(f"\n所有数据处理完成！每张表生成 {num_records} 条记录")
        print(f"总数据量：{num_records * 5} 条记录")

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