import pymysql
from faker import Faker
import random
import time
import numpy as np
from datetime import datetime, timedelta

# 初始化Faker和随机种子（保证数据可复现）
fake = Faker('zh_CN')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# 创建随机数生成器
rng = np.random.default_rng(42)

# 数据库配置（请根据实际环境修改）
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': 'root',
    'database': 'gb_gmall_06',
    'charset': 'utf8mb4'
}

# 全局常量（时间范围：2025-08-08往前30天）
END_DATE_STR = '2025-08-08'
DAYS_BEFORE = 30
START_DATE = datetime.strptime(END_DATE_STR, '%Y-%m-%d') - timedelta(days=DAYS_BEFORE)  # 2025-07-09
END_DATE = datetime.strptime(END_DATE_STR, '%Y-%m-%d')  # 2025-08-08
SECONDS_IN_30_DAYS = DAYS_BEFORE * 24 * 3600  # 30天的总秒数

# 基础数据字典（用于生成真实名称和属性）
## 类目体系（带ID映射）
CATEGORIES = {
    "1000": {"name": "数码电子", "children": {
        "1001": "手机", "1002": "电脑", "1003": "平板", "1004": "相机", "1005": "耳机"
    }},
    "2000": {"name": "服装鞋帽", "children": {
        "2001": "男装", "2002": "女装", "2003": "童装", "2004": "鞋类", "2005": "配饰"
    }},
    "3000": {"name": "家居日用", "children": {
        "3001": "家纺", "3002": "厨具", "3003": "清洁", "3004": "收纳", "3005": "装饰"
    }},
    "4000": {"name": "美妆护肤", "children": {
        "4001": "护肤", "4002": "彩妆", "4003": "香水", "4004": "美发", "4005": "工具"
    }},
    "5000": {"name": "食品生鲜", "children": {
        "5001": "零食", "5002": "生鲜", "5003": "粮油", "5004": "饮料", "5005": "保健品"
    }}
}
# 所有类目ID和名称映射（用于快速查询）
ALL_CATEGORY_IDS = []
ALL_CATEGORY_NAMES = []
for l1_id, l1_info in CATEGORIES.items():
    ALL_CATEGORY_IDS.append(l1_id)
    ALL_CATEGORY_NAMES.append(l1_info["name"])
    for l2_id, l2_name in l1_info["children"].items():
        ALL_CATEGORY_IDS.append(l2_id)
        ALL_CATEGORY_NAMES.append(l2_name)

## 品牌体系（带ID映射）
BRANDS = {
    "b001": "华为", "b002": "苹果", "b003": "小米", "b004": "OPPO", "b005": "vivo",
    "b006": "三星", "b007": "荣耀", "b008": "联想", "b009": "戴尔", "b010": "惠普",
    "b011": "ZARA", "b012": "H&M", "b013": "优衣库", "b014": "兰蔻", "b015": "雅诗兰黛",
    "b016": "三只松鼠", "b017": "良品铺子", "b018": "百草味", "b019": "罗莱", "b020": "富安娜"
}
BRAND_IDS = list(BRANDS.keys())
BRAND_NAMES = list(BRANDS.values())

## 其他枚举值
# 商品属性
COLORS = ["黑色", "白色", "红色", "蓝色", "绿色", "黄色", "粉色", "灰色"]
SIZES = ["S", "M", "L", "XL", "XXL", "均码", "36", "37", "38", "39", "40"]
# 支付方式
PAYMENT_TYPES = ["alipay", "wechat", "unionpay", "credit_card", "balance"]
# 支付渠道
PAY_CHANNELS = ["APP", "PC", "WAP"]
# 店铺类型
STORE_TYPES = ["旗舰店", "专卖店", "专营店"]
# 平台来源
PLATFORMS = ["天猫", "淘宝", "京东", "拼多多"]
# 新品类型
NEW_TYPES = ["普通新品", "小黑盒新品"]
# 审核状态
AUDIT_STATUSES = ["待审", "通过", "驳回"]


# 工具函数：生成随机时间（30天内）
def generate_random_times(n):
    """生成结束日期前30天内的随机时间（精确到秒）"""
    random_seconds = rng.uniform(0, SECONDS_IN_30_DAYS, n)
    return [START_DATE + timedelta(seconds=float(s)) for s in random_seconds]


# 工具函数：生成随机日期（30天内）
def generate_random_dates(n):
    """生成结束日期前30天内的随机日期（仅日期部分）"""
    # 关键修复：将numpy.int64转换为Python int
    return [START_DATE + timedelta(days=int(d)) for d in rng.integers(0, DAYS_BEFORE, n)]


# 数据库连接函数
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


# 1. 生成商品基础信息表 (sku_base_info)
def generate_sku_base_info(num_rows):
    print(f"开始生成 {num_rows} 条商品基础信息数据...")

    # 生成基础ID（确保唯一性）
    sku_ids = rng.integers(10000000, 100000000, size=num_rows, dtype=np.int64)  # 8位商品货号
    store_ids = rng.integers(10000, 100000, size=num_rows, dtype=np.int64)  # 5位店铺ID
    supplier_ids = rng.integers(50000, 100000, size=num_rows, dtype=np.int64)  # 5位供应商ID

    # 生成类目相关字段
    category_ids = []
    category_names = []
    category_paths = []
    for _ in range(num_rows):
        # 随机选择一个二级类目
        l1_id = random.choice(list(CATEGORIES.keys()))
        l1_name = CATEGORIES[l1_id]["name"]
        l2_id = random.choice(list(CATEGORIES[l1_id]["children"].keys()))
        l2_name = CATEGORIES[l1_id]["children"][l2_id]

        category_ids.append(l2_id)  # 类目ID用二级类目ID
        category_names.append(l2_name)  # 类目名称用二级类目名称
        category_paths.append(f"{l1_name}>{l2_name}")  # 类目路径：一级>二级

    # 生成品牌相关字段
    brand_ids = rng.choice(BRAND_IDS, num_rows)
    brand_names = [BRANDS[bid] for bid in brand_ids]

    # 生成商品标题（真实名称组合）
    sku_titles = []
    for i in range(num_rows):
        l2_name = category_names[i]
        brand_name = brand_names[i]
        model = random.choice(["Pro", "Max", "Plus", "标准版", "青春版", "旗舰版"])
        color = random.choice(COLORS)
        sku_titles.append(f"{brand_name} {l2_name} {model} ({color})")

    # 生成图片URL
    sku_main_images = [f"https://img.example.com/sku/{sku_id}/main.jpg" for sku_id in sku_ids]
    sku_sub_images = []
    for sku_id in sku_ids:
        sub_imgs = [f"https://img.example.com/sku/{sku_id}/sub{i}.jpg" for i in range(1, 4)]
        sku_sub_images.append(','.join(sub_imgs))  # 3张辅图用逗号分隔

    # 生成价格相关字段（市场价>销售价>成本价）
    market_prices = np.round(rng.uniform(50, 5000, num_rows), 2)
    sale_prices = np.round(market_prices * rng.uniform(0.6, 0.95, num_rows), 2)  # 销售价为市场价的6-9.5折
    cost_prices = np.round(sale_prices * rng.uniform(0.5, 0.8, num_rows), 2)  # 成本价为销售价的5-8折

    # 生成其他属性
    weights = np.round(rng.uniform(0.1, 10, num_rows), 2)  # 重量（kg）
    colors = rng.choice(COLORS, num_rows)
    sizes = rng.choice(SIZES, num_rows)
    store_names = [f"{brand_names[i]}{random.choice(STORE_TYPES)}" for i in range(num_rows)]  # 店铺名称=品牌+类型

    # 生成时间字段（创建时间≤上架时间≤更新时间）
    create_times = generate_random_times(num_rows)
    putaway_times = [ct + timedelta(hours=random.randint(1, 72)) for ct in create_times]  # 上架时间比创建晚1-72小时
    update_times = [pt + timedelta(days=random.randint(0, 15)) for pt in putaway_times]  # 更新时间比上架晚0-15天

    # 生成状态字段
    is_online = rng.choice([1, 0], num_rows, p=[0.9, 0.1])  # 90%上架
    is_delete = rng.choice([0, 1], num_rows, p=[0.99, 0.01])  # 1%已删除

    # 组装数据
    data = []
    for i in range(num_rows):
        # 修复model变量作用域问题
        current_model = random.choice(["Pro", "Max", "Plus", "标准版", "青春版", "旗舰版"])
        data.append((
            str(sku_ids[i]),          # sku_id
            sku_titles[i],            # sku_title
            sku_main_images[i],       # sku_main_image
            sku_sub_images[i],        # sku_sub_images
            category_ids[i],          # category_id
            category_names[i],        # category_name
            category_paths[i],        # category_path
            brand_ids[i],             # brand_id
            brand_names[i],           # brand_name
            f"{current_model}_{colors[i]}_{sizes[i]}",  # product_model（修复变量作用域）
            float(market_prices[i]),  # market_price
            float(sale_prices[i]),    # sale_price
            float(cost_prices[i]),    # cost_price
            float(weights[i]),        # weight
            colors[i],                # color
            sizes[i],                 # size
            str(store_ids[i]),        # store_id
            store_names[i],           # store_name
            str(supplier_ids[i]),     # supplier_id
            create_times[i],          # create_time
            update_times[i],          # update_time
            putaway_times[i],         # putaway_time
            int(is_online[i]),        # is_online
            int(is_delete[i])         # is_delete
        ))
        # 进度提示
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条商品基础信息数据")
    return data


# 2. 生成商品上新标签表 (sku_new_arrival_tag)
def generate_sku_new_arrival_tag(num_rows, sku_id_list):
    print(f"开始生成 {num_rows} 条商品上新标签数据...")

    # 生成基础ID
    ids = rng.integers(100000000, 1000000000, size=num_rows, dtype=np.int64)  # 9位记录ID
    sku_ids = rng.choice(sku_id_list, num_rows)  # 关联商品ID（从商品表中随机选择）
    operator_ids = rng.integers(1000, 10000, size=num_rows, dtype=np.int64)  # 4位操作人ID

    # 生成时间字段
    putaway_times = generate_random_times(num_rows)  # 上架时间（30天内）
    create_times = [pt - timedelta(minutes=random.randint(10, 120)) for pt in putaway_times]  # 创建时间比上架早10-120分钟
    update_times = [ct + timedelta(days=random.randint(0, 3)) for ct in create_times]  # 更新时间比创建晚0-3天

    # 生成新品标签相关字段
    new_types = rng.choice(NEW_TYPES, num_rows, p=[0.8, 0.2])  # 80%普通新品，20%小黑盒
    is_tmall_new = rng.choice([1, 0], num_rows, p=[0.3, 0.7])  # 30%是天猫新品
    new_tag_audit_statuses = rng.choice(AUDIT_STATUSES, num_rows, p=[0.1, 0.85, 0.05])  # 85%通过审核
    # 关键修复：将numpy数组转换为Python列表（确保元素为int类型）
    new_tag_valid_days = [int(d) for d in rng.integers(7, 90, num_rows)]  # 标签有效期7-90天

    # 计算标签生效/失效时间（通过审核后生效）
    new_tag_start_times = []
    new_tag_end_times = []
    audit_times = []
    reject_reasons = []
    for i in range(num_rows):
        if new_tag_audit_statuses[i] == "通过":
            # 审核通过：生效时间=审核时间，失效时间=生效+有效期
            audit_time = create_times[i] + timedelta(hours=random.randint(1, 24))
            start_time = audit_time
            # 关键修复：使用int类型的天数（已在上方转换）
            end_time = start_time + timedelta(days=new_tag_valid_days[i])
            reject_reason = ""
        elif new_tag_audit_statuses[i] == "驳回":
            # 审核驳回：无生效/失效时间，有驳回原因
            audit_time = create_times[i] + timedelta(hours=random.randint(1, 24))
            start_time = None
            end_time = None
            reject_reason = random.choice(["资质不全", "图片不符合规范", "信息填写错误", "非新品"])
        else:  # 待审
            audit_time = None
            start_time = None
            end_time = None
            reject_reason = ""
        new_tag_start_times.append(start_time)
        new_tag_end_times.append(end_time)
        audit_times.append(audit_time)
        reject_reasons.append(reject_reason)

    # 平台来源
    platforms = rng.choice(PLATFORMS, num_rows)

    # 组装数据
    data = []
    for i in range(num_rows):
        data.append((
            str(ids[i]),                  # id
            sku_ids[i],                   # sku_id
            putaway_times[i],             # putaway_time
            new_types[i],                 # new_type
            int(is_tmall_new[i]),         # is_tmall_new
            new_tag_audit_statuses[i],    # new_tag_audit_status
            new_tag_valid_days[i],        # new_tag_valid_days
            new_tag_start_times[i],       # new_tag_start_time
            new_tag_end_times[i],         # new_tag_end_time
            str(operator_ids[i]),         # operator_id
            audit_times[i],               # audit_time
            reject_reasons[i],            # reject_reason
            platforms[i],                 # platform
            create_times[i],              # create_time
            update_times[i]               # update_time
        ))
        # 进度提示
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条商品上新标签数据")
    return data


# 3. 生成商品支付明细宽表 (sku_payment_detail)
def generate_sku_payment_detail(num_rows, sku_id_list, store_id_list):
    print(f"开始生成 {num_rows} 条商品支付明细数据...")

    # 生成基础ID
    payment_ids = rng.integers(1000000000, 10000000000, size=num_rows, dtype=np.int64)  # 10位支付单ID
    order_ids = rng.integers(2000000000, 20000000000, size=num_rows, dtype=np.int64)  # 10位订单ID（与支付ID区分）
    user_ids = rng.integers(5000000, 10000000, size=num_rows, dtype=np.int64)  # 7位用户ID
    promotion_ids = [str(rng.integers(90000, 100000)) if random.random() < 0.6 else "" for _ in range(num_rows)]  # 60%有促销

    # 关联字段
    sku_ids = rng.choice(sku_id_list, num_rows)  # 关联商品
    store_ids = rng.choice(store_id_list, num_rows)  # 关联店铺

    # 支付相关字段
    payment_quantities = [int(q) for q in rng.integers(1, 5, num_rows)]  # 修复为int类型
    unit_prices = np.round(rng.uniform(50, 2000, num_rows), 2)  # 单价50-2000元
    payment_amounts = np.round(unit_prices * payment_quantities * rng.uniform(0.8, 1.0), 2)  # 支付金额=单价*数量*折扣
    payment_times = generate_random_times(num_rows)  # 支付时间（30天内）
    payment_types = rng.choice(PAYMENT_TYPES, num_rows)
    payment_status = [int(s) for s in rng.choice([1, 0], num_rows, p=[0.98, 0.02])]  # 修复为int类型

    # 退款相关字段（仅2%有退款）
    refund_status = []
    refund_amounts = []
    refund_times = []
    for i in range(num_rows):
        if random.random() < 0.02:  # 2%概率退款
            refund_status.append(1)
            refund_amount = np.round(payment_amounts[i] * rng.uniform(0.5, 1.0), 2)  # 退款金额为支付金额的50%-100%
            refund_amounts.append(refund_amount)
            refund_time = payment_times[i] + timedelta(days=random.randint(1, 15))  # 退款时间比支付晚1-15天
            refund_times.append(refund_time)
        else:
            refund_status.append(0)
            refund_amounts.append(0.00)
            refund_times.append(None)

    # 其他字段
    channels = rng.choice(PAY_CHANNELS, num_rows)
    create_times = [pt - timedelta(minutes=random.randint(0, 5)) for pt in payment_times]  # 创建时间略早于支付时间

    # 组装数据
    data = []
    for i in range(num_rows):
        data.append((
            str(payment_ids[i]),        # payment_id
            str(order_ids[i]),          # order_id
            sku_ids[i],                 # sku_id
            str(user_ids[i]),           # user_id
            float(payment_amounts[i]),  # payment_amount
            payment_quantities[i],      # payment_quantity（已修复为int）
            payment_times[i],           # payment_time
            payment_types[i],           # payment_type
            payment_status[i],          # payment_status（已修复为int）
            refund_status[i],           # refund_status
            float(refund_amounts[i]),   # refund_amount
            refund_times[i],            # refund_time
            store_ids[i],               # store_id
            promotion_ids[i],           # promotion_id
            channels[i],                # channel
            create_times[i]             # create_time
        ))
        # 进度提示
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条商品支付明细数据")
    return data


# 4. 生成商品全年上新记录表 (sku_yearly_new_record)
def generate_sku_yearly_new_record(num_rows, sku_id_list):
    print(f"开始生成 {num_rows} 条商品全年上新记录数据...")

    # 生成基础ID
    ids = rng.integers(300000000, 400000000, size=num_rows, dtype=np.int64)  # 9位记录ID
    sku_ids = rng.choice(sku_id_list, num_rows)  # 关联商品

    # 上架日期相关字段
    putaway_dates = generate_random_dates(num_rows)  # 上架日期（30天内）
    putaway_years = [d.year for d in putaway_dates]
    putaway_months = [d.month for d in putaway_dates]
    putaway_quarters = [(m - 1) // 3 + 1 for m in putaway_months]  # 季度（1-4）
    putaway_weeks = [d.isocalendar()[1] for d in putaway_dates]  # 周数（ISO周）

    # 商品相关字段
    new_types = rng.choice(NEW_TYPES, num_rows, p=[0.8, 0.2])
    initial_sale_prices = np.round(rng.uniform(50, 2000, num_rows), 2)  # 上架时售价
    initial_stocks = [int(s) for s in rng.integers(100, 1000, num_rows)]  # 修复为int类型

    # 首次支付日期（70%有首次支付）
    first_payment_dates = []
    for d in putaway_dates:
        if random.random() < 0.7:  # 70%概率有首次支付
            delay_days = random.randint(0, 10)  # 上架后0-10天内首次支付
            first_payment_dates.append(d + timedelta(days=delay_days))
        else:
            first_payment_dates.append(None)

    # 扩展字段（类目ID和品牌ID）
    category_ids = rng.choice(ALL_CATEGORY_IDS, num_rows)
    brand_ids = rng.choice(BRAND_IDS, num_rows)

    # 创建时间（与上架日期相同）
    create_times = [datetime.combine(d, datetime.min.time()) for d in putaway_dates]

    # 组装数据
    data = []
    for i in range(num_rows):
        data.append((
            str(ids[i]),                # id
            sku_ids[i],                 # sku_id
            putaway_dates[i],           # putaway_date
            putaway_years[i],           # putaway_year
            putaway_months[i],          # putaway_month
            putaway_quarters[i],        # putaway_quarter
            putaway_weeks[i],           # putaway_week
            new_types[i],               # new_type
            float(initial_sale_prices[i]),  # initial_sale_price
            initial_stocks[i],          # initial_stock（已修复为int）
            first_payment_dates[i],     # first_payment_date
            category_ids[i],            # category_id
            brand_ids[i],               # brand_id
            create_times[i]             # create_time
        ))
        # 进度提示
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条商品全年上新记录数据")
    return data


# 5. 生成店铺基础信息表 (store_base_info)
def generate_store_base_info(num_rows):
    print(f"开始生成 {num_rows} 条店铺基础信息数据...")

    # 生成基础ID
    store_ids = rng.integers(10000, 100000, size=num_rows, dtype=np.int64)  # 5位店铺ID
    seller_ids = rng.integers(6000000, 7000000, size=num_rows, dtype=np.int64)  # 7位卖家ID

    # 店铺名称（真实名称组合）
    store_names = []
    for i in range(num_rows):
        brand_name = random.choice(BRAND_NAMES)
        store_type = random.choice(STORE_TYPES)
        main_category = random.choice(ALL_CATEGORY_NAMES)
        store_names.append(f"{brand_name}{main_category}{store_type}")

    # 店铺属性
    store_types = rng.choice(STORE_TYPES, num_rows)
    platforms = rng.choice(PLATFORMS, num_rows)
    category_mains = rng.choice(ALL_CATEGORY_NAMES, num_rows)
    levels = [int(l) for l in rng.integers(1, 5, num_rows)]  # 修复为int类型

    # 时间字段（开店日期早于当前30天，创建/更新时间基于开店日期）
    open_dates = [START_DATE - timedelta(days=random.randint(30, 365*3)) for _ in range(num_rows)]  # 开店30天-3年前
    create_times = [datetime.combine(d, datetime.min.time()) for d in open_dates]  # 创建时间=开店日期
    update_times = [ct + timedelta(days=random.randint(0, 180)) for ct in create_times]  # 更新时间比创建晚0-180天

    # 联系人信息（使用Faker生成真实姓名和电话）
    contact_persons = [fake.name() for _ in range(num_rows)]
    contact_phones = [fake.phone_number() for _ in range(num_rows)]

    # 组装数据
    data = []
    for i in range(num_rows):
        data.append((
            str(store_ids[i]),          # store_id
            store_names[i],             # store_name
            store_types[i],             # store_type
            str(seller_ids[i]),         # seller_id
            platforms[i],               # platform
            category_mains[i],          # category_main
            open_dates[i],              # open_date
            levels[i],                  # level（已修复为int）
            contact_persons[i],         # contact_person
            contact_phones[i],          # contact_phone
            create_times[i],            # create_time
            update_times[i]             # update_time
        ))
        # 进度提示
        if (i + 1) % 100000 == 0:
            print(f"已生成 {i + 1} 条店铺基础信息数据")
    return data


def main():
    start_time = time.time()
    conn = None
    try:
        conn = create_connection()
        num_records = 1200000  # 每张表120万条数据
        print(f"数据时间范围：{START_DATE.strftime('%Y-%m-%d')} 至 {END_DATE_STR}")

        # 1. 生成商品基础信息表（需优先生成，供其他表关联）
        print("\n===== 生成商品基础信息表 (sku_base_info) =====")
        sku_base_data = generate_sku_base_info(num_records)
        # 提取生成的sku_id和store_id（用于后续表关联）
        sku_id_list = [str(row[0]) for row in sku_base_data]  # row[0]是sku_id
        store_id_list = [str(row[16]) for row in sku_base_data]  # row[16]是store_id
        # 插入数据
        batch_insert(conn, 'sku_base_info',
                     ['sku_id', 'sku_title', 'sku_main_image', 'sku_sub_images', 'category_id',
                      'category_name', 'category_path', 'brand_id', 'brand_name', 'product_model',
                      'market_price', 'sale_price', 'cost_price', 'weight', 'color', 'size',
                      'store_id', 'store_name', 'supplier_id', 'create_time', 'update_time',
                      'putaway_time', 'is_online', 'is_delete'],
                     sku_base_data)

        # 2. 生成店铺基础信息表
        print("\n===== 生成店铺基础信息表 (store_base_info) =====")
        store_base_data = generate_store_base_info(num_records)
        batch_insert(conn, 'store_base_info',
                     ['store_id', 'store_name', 'store_type', 'seller_id', 'platform',
                      'category_main', 'open_date', 'level', 'contact_person', 'contact_phone',
                      'create_time', 'update_time'],
                     store_base_data)

        # 3. 生成商品上新标签表（关联商品表的sku_id）
        print("\n===== 生成商品上新标签表 (sku_new_arrival_tag) =====")
        sku_new_tag_data = generate_sku_new_arrival_tag(num_records, sku_id_list)
        batch_insert(conn, 'sku_new_arrival_tag',
                     ['id', 'sku_id', 'putaway_time', 'new_type', 'is_tmall_new',
                      'new_tag_audit_status', 'new_tag_valid_days', 'new_tag_start_time',
                      'new_tag_end_time', 'operator_id', 'audit_time', 'reject_reason',
                      'platform', 'create_time', 'update_time'],
                     sku_new_tag_data)

        # 4. 生成商品支付明细宽表（关联商品表和店铺表）
        print("\n===== 生成商品支付明细宽表 (sku_payment_detail) =====")
        sku_payment_data = generate_sku_payment_detail(num_records, sku_id_list, store_id_list)
        batch_insert(conn, 'sku_payment_detail',
                     ['payment_id', 'order_id', 'sku_id', 'user_id', 'payment_amount',
                      'payment_quantity', 'payment_time', 'payment_type', 'payment_status',
                      'refund_status', 'refund_amount', 'refund_time', 'store_id', 'promotion_id',
                      'channel', 'create_time'],
                     sku_payment_data)

        # 5. 生成商品全年上新记录表（关联商品表的sku_id）
        print("\n===== 生成商品全年上新记录表 (sku_yearly_new_record) =====")
        sku_yearly_data = generate_sku_yearly_new_record(num_records, sku_id_list)
        batch_insert(conn, 'sku_yearly_new_record',
                     ['id', 'sku_id', 'putaway_date', 'putaway_year', 'putaway_month',
                      'putaway_quarter', 'putaway_week', 'new_type', 'initial_sale_price',
                      'initial_stock', 'first_payment_date', 'category_id', 'brand_id',
                      'create_time'],
                     sku_yearly_data)

        print(f"\n所有数据生成完成！共生成 {num_records * 5} 条记录（5张表，各{num_records}条）")

    except Exception as e:
        print(f"执行错误: {e}")
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
        print(f"总耗时: {int(total_time//60)}分{total_time%60:.2f}秒")


if __name__ == "__main__":
    main()