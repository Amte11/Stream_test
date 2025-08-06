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

# 枚举值定义
BEHAVIOR_TYPES = ['view', 'favor', 'cart']  # 用户行为类型
PAY_CHANNELS = ['alipay', 'wechat', 'unionpay', 'jdpay']  # 支付渠道
SHOP_TYPES = ['旗舰店', '专卖店', '专营店', '自营店']  # 店铺类型
REG_CHANNELS = ['app', 'web']  # 用户注册渠道
CATEGORY_L1 = ['数码电子', '服装鞋包', '家居日用', '食品饮料', '美妆个护']  # 一级类目
CATEGORY_L2 = {  # 二级类目（对应一级）
    '数码电子': ['手机', '电脑', '耳机', '相机'],
    '服装鞋包': ['男装', '女装', '运动鞋', '背包'],
    '家居日用': ['厨具', '家纺', '清洁用品', '收纳'],
    '食品饮料': ['零食', '饮料', '粮油', '生鲜'],
    '美妆个护': ['护肤品', '彩妆', '香水', '洗护']
}
CATEGORY_L3 = {  # 三级类目（示例，对应二级）
    '手机': ['智能手机', '老人机', '翻新机'],
    '男装': ['T恤', '裤子', '外套', '卫衣'],
    '厨具': ['锅具', '餐具', '小家电', '刀具']
}

# 时间范围配置（2025-08-06往前30天）
END_DATE_STR = '2025-08-06'  # 结束日期
DAYS_BEFORE = 30  # 往前推30天
END_DATE = datetime.strptime(END_DATE_STR, '%Y-%m-%d')
START_DATE = END_DATE - timedelta(days=DAYS_BEFORE)

# 创建数据库连接
def create_connection():
    try:
        conn = pymysql.connect(** db_config)
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

# 生成随机时间（指定时间范围内）
def generate_random_time():
    """生成START_DATE到END_DATE之间的随机时间（含时分秒）"""
    time_delta = (END_DATE - START_DATE).total_seconds()
    random_seconds = random.uniform(0, time_delta)
    return START_DATE + timedelta(seconds=random_seconds)

# 生成随机日期（仅日期部分）
def generate_random_date():
    """生成START_DATE到END_DATE之间的随机日期"""
    day_delta = (END_DATE - START_DATE).days
    random_days = random.randint(0, day_delta)
    return START_DATE + timedelta(days=random_days)

# 生成ID池（用于关联表的ID复用）
def generate_id_pool(prefix, count):
    return [f"{prefix}_{str(i).zfill(8)}" for i in range(1, count + 1)]

# 1. 生成用户行为日志表（user_behavior_log）数据
def generate_user_behavior_log(num_rows, user_ids, goods_ids):
    data = []
    for i in range(num_rows):
        log_id = f"log_{str(i).zfill(10)}"
        user_id = random.choice(user_ids)
        session_id = f"session_{fake.uuid4().split('-')[0]}"
        goods_id = random.choice(goods_ids)
        behavior_type = random.choice(BEHAVIOR_TYPES)
        behavior_time = generate_random_time()
        device_id = f"device_{fake.md5()[:16]}"
        app_version = f"v{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,99)}"
        page_url = f"/goods/detail?id={goods_id}"
        refer_page = random.choice(['/home', '/category', f"/goods/detail?id={random.choice(goods_ids)}", None])
        refer_goods_id = random.choice(goods_ids) if random.random() > 0.3 else None
        stay_duration = random.randint(5, 300) if behavior_type == 'view' else None
        dt = behavior_time.strftime('%Y%m%d')  # 业务日期（yyyyMMdd）

        data.append((
            log_id, user_id, session_id, goods_id, behavior_type, behavior_time,
            device_id, app_version, page_url, refer_page, refer_goods_id,
            stay_duration, dt
        ))

        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条用户行为日志数据")
    return data

# 2. 生成订单交易事实表（order_trade_fact）数据
def generate_order_trade_fact(num_rows, user_ids, shop_ids):
    data = []
    for i in range(num_rows):
        order_id = f"order_{str(i).zfill(10)}"
        trade_no = f"trade_{fake.uuid4().replace('-','')[:20]}"
        user_id = random.choice(user_ids)
        shop_id = random.choice(shop_ids)
        pay_time = generate_random_time()
        total_amount = round(random.uniform(50, 2000), 2)
        discount_amount = round(total_amount * random.uniform(0, 0.3), 2)
        payment_amount = round(total_amount - discount_amount, 2)
        pay_channel = random.choice(PAY_CHANNELS)
        pay_status = 1 if random.random() > 0.05 else 2  # 95%支付成功
        dt = pay_time.strftime('%Y%m%d')

        data.append((
            order_id, trade_no, user_id, shop_id, pay_time, total_amount,
            discount_amount, payment_amount, pay_channel, pay_status, dt
        ))

        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条订单交易事实数据")
    return data

# 3. 生成订单商品明细表（order_goods_detail）数据
def generate_order_goods_detail(num_rows, order_ids, goods_ids):
    data = []
    for i in range(num_rows):
        detail_id = f"detail_{str(i).zfill(10)}"
        order_id = random.choice(order_ids)
        goods_id = random.choice(goods_ids)
        goods_quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 1000), 2)
        total_price = round(unit_price * goods_quantity, 2)
        promotion_info = random.choice(['满减', '折扣', '优惠券', '无'])
        dt = generate_random_date().strftime('%Y%m%d')

        data.append((
            detail_id, order_id, goods_id, goods_quantity, unit_price,
            total_price, promotion_info, dt
        ))

        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条订单商品明细数据")
    return data

# 4. 生成商品核心属性表（goods_core_attr）数据
def generate_goods_core_attr(num_rows, shop_ids):
    data = []
    for i in range(num_rows):
        goods_id = f"goods_{str(i).zfill(8)}"
        shop_id = random.choice(shop_ids)
        goods_name = fake.word(ext_word_list=CATEGORY_L2['数码电子'] + CATEGORY_L2['服装鞋包']) + fake.random_int(100, 999)
        category_l1 = random.choice(CATEGORY_L1)
        category_l2 = random.choice(CATEGORY_L2[category_l1])
        category_l3 = random.choice(CATEGORY_L3.get(category_l2, [category_l2 + '子分类']))
        is_traffic_item = 1 if random.random() < 0.2 else 0  # 20%为引流品
        is_top_seller = 1 if random.random() < 0.15 else 0  # 15%为热销品
        base_price = round(random.uniform(10, 5000), 2)
        goods_status = 1 if random.random() > 0.1 else 0  # 90%上架
        create_time = fake.date_time_between(start_date='-365d', end_date='-30d')  # 30天前创建
        update_time = fake.date_time_between(start_date=create_time, end_date='-1d')  # 创建后更新

        data.append((
            goods_id, shop_id, goods_name, category_l1, category_l2, category_l3,
            is_traffic_item, is_top_seller, base_price, goods_status,
            create_time, update_time
        ))

        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条商品核心属性数据")
    return data

# 5. 生成店铺基础信息表（shop_base_info）数据
def generate_shop_base_info(num_rows, seller_ids):
    data = []
    for i in range(num_rows):
        shop_id = f"shop_{str(i).zfill(6)}"
        shop_name = fake.company() + random.choice(['旗舰店', '专卖店'])
        seller_user_id = random.choice(seller_ids)
        shop_level = random.randint(1, 5)
        shop_type = random.choice(SHOP_TYPES)
        main_category = random.choice(CATEGORY_L1)
        create_date = fake.date_between(start_date='-10y', end_date='-1y')  # 1-10年前开店

        data.append((
            shop_id, shop_name, seller_user_id, shop_level, shop_type,
            main_category, create_date
        ))

        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条店铺基础信息数据")
    return data

# 6. 生成用户基础信息表（user_base_info）数据
def generate_user_base_info(num_rows):
    data = []
    for i in range(num_rows):
        user_id = f"user_{str(i).zfill(8)}"
        user_name = fake.name()
        reg_channel = random.choice(REG_CHANNELS)
        reg_date = fake.date_between(start_date='-5y', end_date='-1d')  # 1-5年前注册
        last_login_time = fake.date_time_between(start_date=reg_date, end_date=END_DATE)
        user_level = random.randint(1, 5)

        data.append((
            user_id, user_name, reg_channel, reg_date, last_login_time, user_level
        ))

        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条用户基础信息数据")
    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()
        num_records = 1500000  # 每张表150万条数据

        # 生成关联ID池（用于多表关联）
        print("生成关联ID池...")
        user_ids = generate_id_pool("user", 500000)  # 50万用户
        shop_ids = generate_id_pool("shop", 50000)   # 5万店铺
        goods_ids = generate_id_pool("goods", 100000) # 10万商品
        order_ids = generate_id_pool("order", 2000000) # 200万订单（覆盖订单表）
        seller_ids = generate_id_pool("seller", 100000) # 10万商家用户
        print(f"ID池生成完成：用户{len(user_ids)}个，店铺{len(shop_ids)}个，商品{len(goods_ids)}个")
        print(f"数据时间范围：{START_DATE.strftime('%Y-%m-%d')} 至 {END_DATE.strftime('%Y-%m-%d')}")

        # 1. 生成用户行为日志表数据
        print("\n开始生成用户行为日志表（user_behavior_log）...")
        behavior_data = generate_user_behavior_log(num_records, user_ids, goods_ids)
        batch_insert(conn, 'user_behavior_log',
                     ['log_id', 'user_id', 'session_id', 'goods_id', 'behavior_type', 'behavior_time',
                      'device_id', 'app_version', 'page_url', 'refer_page', 'refer_goods_id',
                      'stay_duration', 'dt'],
                     behavior_data, batch_size=5000)

        # 2. 生成订单交易事实表数据
        print("\n开始生成订单交易事实表（order_trade_fact）...")
        trade_data = generate_order_trade_fact(num_records, user_ids, shop_ids)
        batch_insert(conn, 'order_trade_fact',
                     ['order_id', 'trade_no', 'user_id', 'shop_id', 'pay_time', 'total_amount',
                      'discount_amount', 'payment_amount', 'pay_channel', 'pay_status', 'dt'],
                     trade_data, batch_size=5000)

        # 3. 生成订单商品明细表数据
        print("\n开始生成订单商品明细表（order_goods_detail）...")
        detail_data = generate_order_goods_detail(num_records, order_ids, goods_ids)
        batch_insert(conn, 'order_goods_detail',
                     ['detail_id', 'order_id', 'goods_id', 'goods_quantity', 'unit_price',
                      'total_price', 'promotion_info', 'dt'],
                     detail_data, batch_size=5000)

        # 4. 生成商品核心属性表数据
        print("\n开始生成商品核心属性表（goods_core_attr）...")
        goods_data = generate_goods_core_attr(num_records, shop_ids)
        batch_insert(conn, 'goods_core_attr',
                     ['goods_id', 'shop_id', 'goods_name', 'category_l1', 'category_l2', 'category_l3',
                      'is_traffic_item', 'is_top_seller', 'base_price', 'goods_status',
                      'create_time', 'update_time'],
                     goods_data, batch_size=5000)

        # 5. 生成店铺基础信息表数据
        print("\n开始生成店铺基础信息表（shop_base_info）...")
        shop_data = generate_shop_base_info(num_records, seller_ids)
        batch_insert(conn, 'shop_base_info',
                     ['shop_id', 'shop_name', 'seller_user_id', 'shop_level', 'shop_type',
                      'main_category', 'create_date'],
                     shop_data, batch_size=5000)

        # 6. 生成用户基础信息表数据
        print("\n开始生成用户基础信息表（user_base_info）...")
        user_data = generate_user_base_info(num_records)
        batch_insert(conn, 'user_base_info',
                     ['user_id', 'user_name', 'reg_channel', 'reg_date', 'last_login_time', 'user_level'],
                     user_data, batch_size=5000)

        print(f"\n所有数据处理完成！每张表生成 {num_records} 条记录")
        print(f"总数据量：{num_records * 6} 条记录")

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