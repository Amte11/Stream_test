import pymysql
from faker import Faker
import random
import time
import numpy as np
from datetime import datetime, timedelta

# 初始化Faker和随机种子
fake = Faker('zh_CN')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# 创建随机数生成器
rng = np.random.default_rng(42)

# 数据库配置
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': 'root',
    'database': 'gb_gmall_05',
    'charset': 'utf8mb4'
}

# 全局常量
END_DATE_STR = '2025-08-07'  # 新的结束日期
DAYS_BEFORE = 30  # 往前推30天
START_DATE = datetime.strptime(END_DATE_STR, '%Y-%m-%d') - timedelta(days=DAYS_BEFORE)
END_DATE = datetime.strptime(END_DATE_STR, '%Y-%m-%d')
SECONDS_IN_30_DAYS = DAYS_BEFORE * 24 * 3600

# 用户行为类型
BEHAVIOR_TYPES = ['view', 'favor', 'cart']
# APP版本
APP_VERSIONS = ['v1.0.0', 'v1.1.0', 'v1.2.0', 'v2.0.0', 'v2.1.0']
# 支付渠道
PAY_CHANNELS = ['alipay', 'wechat', 'unionpay', 'credit_card', 'balance']
# 店铺类型
SHOP_TYPES = ['旗舰店', '专卖店', '专营店', '自营店', '品牌店']
# 商品类目
CATEGORIES_L1 = ['数码电子', '服装鞋帽', '家居日用', '美妆护肤', '食品生鲜']
CATEGORIES_L2 = {
    '数码电子': ['手机', '电脑', '平板', '相机', '耳机'],
    '服装鞋帽': ['男装', '女装', '童装', '鞋类', '配饰'],
    '家居日用': ['家纺', '厨具', '清洁', '收纳', '装饰'],
    '美妆护肤': ['护肤', '彩妆', '香水', '美发', '工具'],
    '食品生鲜': ['零食', '生鲜', '粮油', '饮料', '保健品']
}
# 用户等级
USER_LEVELS = [1, 2, 3, 4, 5]
# 注册渠道
REG_CHANNELS = ['app', 'web']

# 品牌名称
BRANDS = {
    '手机': ['华为', '苹果', '小米', 'OPPO', 'vivo', '三星', '荣耀'],
    '电脑': ['联想', '戴尔', '惠普', '华硕', '苹果', '宏碁'],
    '平板': ['苹果', '华为', '三星', '小米', '联想'],
    '相机': ['佳能', '尼康', '索尼', '富士', '松下'],
    '耳机': ['索尼', 'Bose', '森海塞尔', '苹果', 'JBL'],
    '男装': ['海澜之家', '杰克琼斯', '李宁', '安踏', '优衣库'],
    '女装': ['ZARA', 'H&M', '优衣库', 'ONLY', 'VERO MODA'],
    '童装': ['巴拉巴拉', '安奈儿', '小猪班纳', '迪士尼', '好孩子'],
    '鞋类': ['耐克', '阿迪达斯', '李宁', '安踏', '新百伦'],
    '配饰': ['卡西欧', '天梭', '施华洛世奇', '潘多拉', '周大福'],
    '家纺': ['罗莱', '富安娜', '水星', '梦洁', '多喜爱'],
    '厨具': ['苏泊尔', '美的', '九阳', '双立人', '爱仕达'],
    '清洁': ['蓝月亮', '威露士', '立白', '滴露', '奥妙'],
    '收纳': ['乐扣乐扣', '太力', '茶花', '天马', '爱丽思'],
    '装饰': ['宜家', '无印良品', 'ZARA HOME', '野兽派', '造作'],
    '护肤': ['兰蔻', '雅诗兰黛', '欧莱雅', '资生堂', 'SK-II'],
    '彩妆': ['MAC', '迪奥', '香奈儿', '圣罗兰', '阿玛尼'],
    '香水': ['香奈儿', '迪奥', '古驰', '祖玛珑', '爱马仕'],
    '美发': ['欧莱雅', '施华蔻', '沙宣', '卡诗', '资生堂'],
    '工具': ['飞利浦', '松下', '博朗', '戴森', '小米'],
    '零食': ['三只松鼠', '良品铺极', '百草味', '来伊份', '洽洽'],
    '生鲜': ['盒马', '每日优鲜', '叮咚买菜', '美团买菜', '京东生鲜'],
    '粮油': ['金龙鱼', '福临门', '鲁花', '中粮', '十月稻田'],
    '饮料': ['可口可乐', '百事可乐', '农夫山泉', '康师傅', '统一'],
    '保健品': ['汤臣倍健', 'Swisse', '安利', '同仁堂', '修正']
}

def create_connection():
    try:
        conn = pymysql.connect(** db_config)
        print("数据库连接成功")
        return conn
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        raise

# 批量插入函数
def batch_insert(conn, table_name, columns, data, batch_size=5000):
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

# 批量生成随机时间
def generate_random_times(n):
    """批量生成结束日期前30天内的随机时间"""
    random_seconds = rng.uniform(0, SECONDS_IN_30_DAYS, n)
    # 将numpy.float64转换为Python float
    return [START_DATE + timedelta(seconds=float(s)) for s in random_seconds]

# 批量生成随机日期
def generate_random_dates(n):
    """批量生成结束日期前30天内的随机日期"""
    # 关键修正：将numpy.int64转换为Python int
    return [START_DATE + timedelta(days=int(d)) for d in rng.integers(0, DAYS_BEFORE, n)]

# 生成用户行为日志表数据
def generate_user_behavior_log(num_rows):
    print(f"开始批量生成 {num_rows} 条用户行为日志数据...")

    # 预生成ID - 使用纯数字
    log_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位日志ID
    user_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位用户ID
    session_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位会话ID
    goods_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位商品ID
    device_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位设备ID

    # 批量生成时间
    times = generate_random_times(num_rows)

    # 批量生成其他字段
    behavior_type_choices = rng.choice(BEHAVIOR_TYPES, num_rows)
    app_version_choices = rng.choice(APP_VERSIONS, num_rows)
    stay_durations = rng.integers(5, 600, num_rows)  # 停留5秒到10分钟

    # 生成页面URL和来源页面
    page_urls = []
    refer_pages = []
    refer_goods_ids = []

    # 预生成所有商品ID列表用于随机选择
    all_goods_ids = goods_ids.tolist()

    for i in range(num_rows):
        # 生成页面URL
        page_type = random.choice(['product', 'category', 'search', 'home', 'cart'])
        if page_type == 'product':
            page_url = f"/product/{goods_ids[i]}"
        elif page_type == 'category':
            category = random.choice(list(CATEGORIES_L2.values()))
            page_url = f"/category/{random.choice(category)}"
        elif page_type == 'search':
            keywords = ['手机', '电脑', '衣服', '鞋子', '化妆品', '食品']
            page_url = f"/search?keyword={random.choice(keywords)}"
        else:
            page_url = f"/{page_type}"

        page_urls.append(page_url)

        # 初始化来源页面和来源商品ID
        refer_page = ''
        refer_goods_id = ''

        # 生成来源页面
        if random.random() < 0.7:  # 70%的概率有来源页面
            refer_type = random.choice(['product', 'category', 'search', 'home'])
            if refer_type == 'product':
                refer_goods_id = str(random.choice(all_goods_ids))
                refer_page = f"/product/{refer_goods_id}"
            elif refer_type == 'category':
                category = random.choice(list(CATEGORIES_L2.values()))
                refer_page = f"/category/{random.choice(category)}"
            elif refer_type == 'search':
                keywords = ['手机', '电脑', '衣服', '鞋子', '化妆品', '食品']
                refer_page = f"/search?keyword={random.choice(keywords)}"
            else:
                refer_page = f"/{refer_type}"

        # 添加到列表
        refer_pages.append(refer_page)
        refer_goods_ids.append(refer_goods_id)

    # 确保所有列表长度一致
    assert len(page_urls) == num_rows
    assert len(refer_pages) == num_rows
    assert len(refer_goods_ids) == num_rows

    # 构建数据
    data = []
    for i in range(num_rows):
        data.append((
            str(log_ids[i]),        # log_id
            str(user_ids[i]),      # user_id
            str(session_ids[i]),    # session_id
            str(goods_ids[i]),      # goods_id
            behavior_type_choices[i],  # behavior_type
            times[i],          # behavior_time
            str(device_ids[i]),     # device_id
            app_version_choices[i],  # app_version
            page_urls[i],      # page_url
            refer_pages[i],    # refer_page
            refer_goods_ids[i],  # refer_goods_id
            int(stay_durations[i])  # stay_duration
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条用户行为日志数据")

    return data

# 生成订单交易事实表数据
def generate_order_trade_fact(num_rows):
    print(f"开始批量生成 {num_rows} 条订单交易数据...")

    # 预生成ID - 使用纯数字
    order_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位订单ID
    trade_nos = rng.integers(100000000000000, 1000000000000000, size=num_rows, dtype=np.int64)  # 15位交易流水号
    user_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位用户ID
    shop_ids = rng.integers(100000, 1000000, size=num_rows, dtype=np.int64)  # 6位店铺ID

    # 批量生成时间
    times = generate_random_times(num_rows)

    # 批量生成金额
    total_amounts = np.round(rng.uniform(50, 5000, num_rows), 2)
    discount_amounts = np.round(total_amounts * rng.uniform(0.05, 0.3, num_rows), 2)
    payment_amounts = total_amounts - discount_amounts

    # 批量生成其他字段
    pay_channel_choices = rng.choice(PAY_CHANNELS, num_rows)
    pay_status_choices = rng.choice([1, 2], num_rows, p=[0.98, 0.02])  # 98%成功，2%失败

    # 构建数据
    data = []
    for i in range(num_rows):
        data.append((
            str(order_ids[i]),        # order_id
            str(trade_nos[i]),        # trade_no
            str(user_ids[i]),         # user_id
            str(shop_ids[i]),         # shop_id
            times[i],            # pay_time
            float(total_amounts[i]),    # total_amount
            float(discount_amounts[i]), # discount_amount
            float(payment_amounts[i]),  # payment_amount
            pay_channel_choices[i],     # pay_channel
            int(pay_status_choices[i])  # pay_status
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条订单交易数据")

    return data

# 生成订单商品明细表数据
def generate_order_goods_detail(num_rows):
    print(f"开始批量生成 {num_rows} 条订单商品明细数据...")

    # 预生成ID - 使用纯数字
    detail_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位明细ID
    order_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位订单ID
    goods_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位商品ID

    # 批量生成数量
    quantities = rng.integers(1, 10, num_rows)

    # 批量生成价格
    unit_prices = np.round(rng.uniform(10, 1000, num_rows), 2)
    total_prices = np.round(unit_prices * quantities, 2)

    # 生成促销信息
    promotion_infos = []
    promotions = ['满减', '折扣', '优惠券', '秒杀', '团购', '无']

    for i in range(num_rows):
        if random.random() < 0.6:  # 60%的概率有促销
            promotion = random.choice(promotions[:-1])
            if promotion == '满减':
                promotion_info = f"{promotion}-满{random.randint(100, 500)}减{random.randint(10, 50)}"
            elif promotion == '折扣':
                promotion_info = f"{promotion}-{random.randint(5, 9)}折"
            else:
                promotion_info = promotion
        else:
            promotion_info = '无'

        promotion_infos.append(promotion_info)

    # 构建数据
    data = []
    for i in range(num_rows):
        data.append((
            str(detail_ids[i]),        # detail_id
            str(order_ids[i]),         # order_id
            str(goods_ids[i]),         # goods_id
            int(quantities[i]),   # goods_quantity
            float(unit_prices[i]), # unit_price
            float(total_prices[i]), # total_price
            promotion_infos[i]   # promotion_info
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条订单商品明细数据")

    return data

# 生成商品核心属性表数据
def generate_goods_core_attr(num_rows):
    print(f"开始批量生成 {num_rows} 条商品核心属性数据...")

    # 预生成ID - 使用纯数字
    goods_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位商品ID
    shop_ids = rng.integers(100000, 1000000, size=num_rows, dtype=np.int64)  # 6位店铺ID

    # 生成商品名称 - 更真实的名称
    goods_names = []

    # 生成类目
    category_l1_choices = rng.choice(CATEGORIES_L1, num_rows)
    category_l2_choices = []
    category_l3_choices = []

    for cat_l1 in category_l1_choices:
        cat_l2_options = CATEGORIES_L2[cat_l1]
        cat_l2 = random.choice(cat_l2_options)
        category_l2_choices.append(cat_l2)

        # 三级类目简单处理
        cat_l3 = f"{cat_l2}_{random.randint(1, 10)}"
        category_l3_choices.append(cat_l3)

    # 构建商品名称
    for i in range(num_rows):
        cat_l2 = category_l2_choices[i]
        brand = random.choice(BRANDS.get(cat_l2, ['品牌']))
        model = f"{random.choice(['Pro', 'Max', 'Plus', '旗舰版', '尊享版'])}"
        goods_name = f"{brand}{cat_l2}{model}"
        goods_names.append(goods_name)

    # 生成其他字段
    is_traffic_items = rng.choice([0, 1], num_rows, p=[0.7, 0.3])  # 30%是引流品
    is_top_sellers = rng.choice([0, 1], num_rows, p=[0.8, 0.2])    # 20%是热销品
    base_prices = np.round(rng.uniform(10, 1000, num_rows), 2)
    goods_statuses = rng.choice([0, 1], num_rows, p=[0.1, 0.9])     # 90%上架

    # 生成时间
    create_times = generate_random_times(num_rows)
    # 修正：将numpy.int64转换为Python int
    update_times = [ct + timedelta(days=int(random.randint(0, 365))) for ct in create_times]

    # 构建数据
    data = []
    for i in range(num_rows):
        data.append((
            str(goods_ids[i]),            # goods_id
            str(shop_ids[i]),             # shop_id
            goods_names[i],          # goods_name
            category_l1_choices[i], # category_l1
            category_l2_choices[i], # category_l2
            category_l3_choices[i], # category_l3
            int(is_traffic_items[i]), # is_traffic_item
            int(is_top_sellers[i]),  # is_top_seller
            float(base_prices[i]),   # base_price
            int(goods_statuses[i]),  # goods_status
            create_times[i],        # create_time
            update_times[i]         # update_time
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条商品核心属性数据")

    return data

# 生成店铺基础信息表数据
def generate_shop_base_info(num_rows):
    print(f"开始批量生成 {num_rows} 条店铺基础信息数据...")

    # 预生成ID - 使用纯数字
    shop_ids = rng.integers(100000, 1000000, size=num_rows, dtype=np.int64)  # 6位店铺ID
    seller_user_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位卖家用户ID

    # 生成店铺名称 - 更真实的名称
    shop_names = []
    prefixes = ["官方", "旗舰", "精品", "品质", "优选", "品牌", "时尚"]
    suffixes = ["专卖店", "专营店", "旗舰店", "自营店", "商城"]
    categories = ["数码", "服装", "家居", "美妆", "食品"]

    for i in range(num_rows):
        brand = random.choice(list(BRANDS.values())[0])  # 随机选择一个品牌
        name = f"{brand}{random.choice(prefixes)}{random.choice(categories)}{random.choice(suffixes)}"
        shop_names.append(name)

    # 生成其他字段
    shop_levels = rng.choice(USER_LEVELS, num_rows)  # 店铺等级1-5
    shop_type_choices = rng.choice(SHOP_TYPES, num_rows)
    main_category_choices = rng.choice(CATEGORIES_L1, num_rows)

    # 生成开店日期
    create_dates = generate_random_dates(num_rows)

    # 构建数据
    data = []
    for i in range(num_rows):
        data.append((
            str(shop_ids[i]),              # shop_id
            shop_names[i],            # shop_name
            str(seller_user_ids[i]),       # seller_user_id
            int(shop_levels[i]),      # shop_level
            shop_type_choices[i],     # shop_type
            main_category_choices[i],  # main_category
            create_dates[i]          # create_date
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条店铺基础信息数据")

    return data

# 生成用户基础信息表数据
def generate_user_base_info(num_rows):
    print(f"开始批量生成 {num_rows} 条用户基础信息数据...")

    # 预生成ID - 使用纯数字
    user_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位用户ID

    # 生成用户名 - 使用Faker生成真实中文姓名
    user_names = [fake.name() for _ in range(num_rows)]

    # 生成其他字段
    reg_channel_choices = rng.choice(REG_CHANNELS, num_rows)
    reg_dates = generate_random_dates(num_rows)
    # 修正：将numpy.int64转换为Python int
    last_login_times = [rd + timedelta(days=int(random.randint(0, 365))) for rd in reg_dates]
    user_levels = rng.choice(USER_LEVELS, num_rows)

    # 构建数据
    data = []
    for i in range(num_rows):
        data.append((
            str(user_ids[i]),            # user_id
            user_names[i],           # user_name
            reg_channel_choices[i],  # reg_channel
            reg_dates[i],            # reg_date
            last_login_times[i],     # last_login_time
            int(user_levels[i])      # user_level
        ))

        # 每10万条打印进度
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条用户基础信息数据")

    return data

def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()
        num_records = 1200000  # 每张表120万条数据

        print(f"数据时间范围：{START_DATE} 至 {END_DATE_STR}")

        # 1. 生成用户行为日志表
        print("\n开始生成用户行为日志表 (user_behavior_log)...")
        behavior_log_data = generate_user_behavior_log(num_records)
        batch_insert(conn, 'user_behavior_log',
                     ['log_id', 'user_id', 'session_id', 'goods_id', 'behavior_type',
                      'behavior_time', 'device_id', 'app_version', 'page_url', 'refer_page',
                      'refer_goods_id', 'stay_duration'],
                     behavior_log_data, batch_size=5000)

        # 2. 生成订单交易事实表
        print("\n开始生成订单交易事实表 (order_trade_fact)...")
        order_trade_data = generate_order_trade_fact(num_records)
        batch_insert(conn, 'order_trade_fact',
                     ['order_id', 'trade_no', 'user_id', 'shop_id', 'pay_time',
                      'total_amount', 'discount_amount', 'payment_amount',
                      'pay_channel', 'pay_status'],
                     order_trade_data, batch_size=5000)

        # 3. 生成订单商品明细表
        print("\n开始生成订单商品明细表 (order_goods_detail)...")
        order_goods_data = generate_order_goods_detail(num_records)
        batch_insert(conn, 'order_goods_detail',
                     ['detail_id', 'order_id', 'goods_id', 'goods_quantity',
                      'unit_price', 'total_price', 'promotion_info'],
                     order_goods_data, batch_size=5000)

        # 4. 生成商品核心属性表
        print("\n开始生成商品核心属性表 (goods_core_attr)...")
        goods_attr_data = generate_goods_core_attr(num_records)
        batch_insert(conn, 'goods_core_attr',
                     ['goods_id', 'shop_id', 'goods_name', 'category_l1',
                      'category_l2', 'category_l3', 'is_traffic_item',
                      'is_top_seller', 'base_price', 'goods_status',
                      'create_time', 'update_time'],
                     goods_attr_data, batch_size=5000)

        # 5. 生成店铺基础信息表
        print("\n开始生成店铺基础信息表 (shop_base_info)...")
        shop_info_data = generate_shop_base_info(num_records)
        batch_insert(conn, 'shop_base_info',
                     ['shop_id', 'shop_name', 'seller_user_id', 'shop_level',
                      'shop_type', 'main_category', 'create_date'],
                     shop_info_data, batch_size=5000)

        # 6. 生成用户基础信息表
        print("\n开始生成用户基础信息表 (user_base_info)...")
        user_info_data = generate_user_base_info(num_records)
        batch_insert(conn, 'user_base_info',
                     ['user_id', 'user_name', 'reg_channel', 'reg_date',
                      'last_login_time', 'user_level'],
                     user_info_data, batch_size=5000)

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