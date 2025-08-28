import pymysql
import random
from datetime import datetime, timedelta
from faker import Faker
import os

# ============= 配置区 =============
# 请修改为您的数据库信息
DB_CONFIG = {
    'host': 'cdh01',      # 数据库地址
    'user': 'root',       # 数据库用户名
    'password': 'root',   # 数据库密码
    'database': 'gd_gmall_04', # 数据库名
    'charset': 'utf8mb4'
}

TARGET_DATE = '2025-08-28'  # 目标日期
TOTAL_ROWS = 50000          # 总共要生成的数据行数
LOG_FILE_PATH = 'Pasted_Text_1756389144278.txt'  # 您提供的日志文件路径
# ================================

# 初始化 Faker 生成器（中文）
fake = Faker('zh_CN')

# 连接数据库
connection = pymysql.connect(**DB_CONFIG)
cursor = connection.cursor()

def execute_insert(sql, data):
    """执行插入语句，包含错误处理"""
    try:
        cursor.execute(sql, data)
    except Exception as e:
        print(f"插入数据时出错: {e}")
        print(f"SQL: {sql}")
        print(f"Data: {data}")
        connection.rollback()
    else:
        connection.commit()

def generate_unique_5digit_id(existing_ids):
    """
    生成一个唯一的5位数字ID。
    通过检查已存在的ID集合来避免冲突。
    """
    while True:
        new_id = random.randint(10000, 99999)
        if new_id not in existing_ids:
            existing_ids.add(new_id)
            return new_id

def generate_base_data():
    """生成基础维度数据：店铺、商品、消费者"""
    print("正在生成基础维度数据...")

    # 使用集合来跟踪已生成的ID，确保唯一性
    existing_shop_ids = set()
    existing_goods_ids = set()
    existing_consumer_ids = set()

    # 假设的店铺ID列表 (生成5个)
    shop_ids = [generate_unique_5digit_id(existing_shop_ids) for _ in range(5)]
    shop_categories = ['服装', '家电', '食品', '美妆', '数码']
    shop_suffixes = ['旗舰店', '专卖店', '官方店', '直营店']

    # 假设的商品ID列表 (生成50个)
    goods_ids = [generate_unique_5digit_id(existing_goods_ids) for _ in range(50)]
    goods_categories = ['男装-牛仔裤', '女装-连衣裙', '家电-空调', '零食-坚果', '手机-旗舰机']

    # 假设的消费者ID列表 (生成500个)
    consumer_ids = [generate_unique_5digit_id(existing_consumer_ids) for _ in range(500)]

    # 1. 插入店铺信息
    for shop_id in shop_ids:
        sql = """
        INSERT IGNORE INTO shop_info 
        (shop_id, merchant_id, shop_name, shop_category, open_date, is_direct_bus, business_scope) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = (
            shop_id,
            generate_unique_5digit_id(existing_shop_ids),  # 商家ID
            f"{fake.company()}{random.choice(shop_suffixes)}",  # 真实店铺名称
            random.choice(shop_categories),  # 店铺品类
            '2023-01-01',  # 开店日期
            random.choice([0, 1]),  # 是否开通直通车
            random.choice(['仅国内', '淘特+跨境'])  # 业务范围
        )
        execute_insert(sql, data)

    # 2. 插入商品信息
    brands = ['李宁', '华为', '格力', '三只松鼠', '完美日记', '小米', '农夫山泉']
    for goods_id in goods_ids:
        sql = """
        INSERT IGNORE INTO goods_info 
        (goods_id, shop_id, goods_name, sku_list, goods_category, shelf_time, is_pre_sale, is_support_pay_later) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = (
            goods_id,
            random.choice(shop_ids),  # 随机分配店铺
            f"{random.choice(brands)}{fake.word()}",  # 真实商品名称，如"华为手机"
            '[{"sku":"S1","color":"红色","size":"L"},{"sku":"S2","color":"蓝色","size":"M"}]',  # SKU列表 (JSON)
            random.choice(goods_categories),  # 商品品类
            f"{TARGET_DATE} 00:00:00",  # 上架时间
            random.choice([0, 1]),  # 是否预售
            random.choice([0, 1])   # 是否支持先用后付
        )
        execute_insert(sql, data)

    # 3. 插入消费者信息
    for consumer_id in consumer_ids:
        first_shopping_time = None
        if random.random() > 0.3:  # 70%的消费者有购物记录
            first_shopping_time = f"{TARGET_DATE} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"

        sql = """
        INSERT IGNORE INTO customer_info 
        (consumer_id, register_time, is_88vip, first_shopping_time, history_shop_cnt) 
        VALUES (%s, %s, %s, %s, %s)
        """
        data = (
            consumer_id,
            f"{TARGET_DATE} 00:00:00",  # 注册时间（简化）
            random.choice([0, 1]),  # 是否88VIP
            first_shopping_time,
            random.randint(0, 10)  # 过去一年购买店铺数
        )
        execute_insert(sql, data)

    # 4. 插入渠道配置 (ID为字符串，保持不变)
    channels = [
        ('taobao_search', '手淘搜索', '搜索', 'Y'),
        ('zhitongche', '直通车', '付费推广', 'Y'),
        ('other', '其他', '自然流量', 'N')
    ]
    for channel in channels:
        sql = """
        INSERT IGNORE INTO channel_config_info 
        (channel_code, channel_name, channel_type, is_visible_in_report) 
        VALUES (%s, %s, %s, %s)
        """
        execute_insert(sql, channel)

    print("基础维度数据生成完成。")
    return shop_ids, goods_ids, consumer_ids

def generate_behavior_data(shop_ids, goods_ids, consumer_ids):
    """生成行为事实数据"""
    print(f"正在生成 {TOTAL_ROWS} 条行为数据...")

    # 定义行为类型和渠道
    behaviors = ['访问', '搜索', '加购', '收藏', '支付', '入会', '咨询']
    visible_channels = ['手淘搜索', '直通车', '其他']
    order_types = ['普通', '预售', '先用后付']

    # 统计各类行为的比例 (访问最多，支付最少)
    behavior_weights = {
        '访问': 40,
        '搜索': 15,
        '加购': 15,
        '收藏': 10,
        '支付': 10,
        '入会': 5,
        '咨询': 5
    }
    behavior_choices = random.choices(behaviors, weights=behavior_weights.values(), k=TOTAL_ROWS)

    devices = ['手机', 'PC', '平板']
    page_types = ['商品页', '首页', '活动页']

    # 为行为事实表的ID也使用唯一性检查
    existing_visitor_ids = set()
    existing_search_ids = set()
    existing_cart_ids = set()
    existing_collect_ids = set()
    existing_membership_ids = set()
    existing_consult_ids = set()
    existing_payment_ids = set()

    def generate_behavior_id(existing_set):
        return generate_unique_5digit_id(existing_set)

    for i in range(TOTAL_ROWS):
        behavior_type = behavior_choices[i]
        consumer_id = random.choice(consumer_ids)
        shop_id = random.choice(shop_ids)
        channel = random.choice(visible_channels)

        # 生成该行为在目标日期内的随机时间
        second_of_day = random.randint(0, 24*3600 - 1)
        behavior_datetime = datetime.strptime(TARGET_DATE, '%Y-%m-%d') + timedelta(seconds=second_of_day)
        behavior_date = behavior_datetime.date()

        # 根据行为类型生成具体数据
        if behavior_type == '访问':
            goods_id = random.choice(goods_ids) if random.random() > 0.1 else 0  # 10%概率访问首页
            page_type = random.choice(page_types)
            sql = """
            INSERT INTO visitor_info_detail 
            (visitor_id, consumer_id, shop_id, goods_id, behavior_time, channel, device_type, visit_page_type, is_new_visitor) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                generate_behavior_id(existing_visitor_ids),  # visitor_id
                consumer_id,
                shop_id,
                goods_id,
                behavior_datetime,
                channel,
                random.choice(devices),
                page_type,
                random.choice([0, 1])
            )
            execute_insert(sql, data)

        elif behavior_type == '搜索':
            keyword = fake.word() if random.random() > 0.2 else None
            click_goods_id = random.choice(goods_ids) if keyword and random.random() > 0.3 else None
            stay_sec = random.randint(0, 120) if keyword else 0
            sql = """
            INSERT INTO search_info_detail 
            (search_id, consumer_id, shop_id, search_keyword, click_goods_id, search_time, search_stay_sec, channel) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                generate_behavior_id(existing_search_ids),  # search_id
                consumer_id,
                shop_id,
                keyword,
                click_goods_id,
                behavior_datetime,
                stay_sec,
                channel
            )
            execute_insert(sql, data)

        elif behavior_type == '加购':
            goods_id = random.choice(goods_ids)
            sku_id = f"SKU{random.randint(1000, 9999)}"
            addcart_num = random.randint(1, 5)
            history_cnt = random.randint(0, 20)
            sql = """
            INSERT INTO cart_info_detail 
            (addcart_id, consumer_id, shop_id, goods_id, sku_id, addcart_time, channel, addcart_num, is_cancel_addcart, history_addcart_cnt) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                generate_behavior_id(existing_cart_ids),  # addcart_id
                consumer_id,
                shop_id,
                goods_id,
                sku_id,
                behavior_datetime,
                channel,
                addcart_num,
                random.choice([0, 1]),
                history_cnt
            )
            execute_insert(sql, data)

        elif behavior_type == '收藏':
            goods_id = random.choice(goods_ids)
            history_cnt = random.randint(0, 15)
            sql = """
            INSERT INTO collect_info_detail 
            (collect_id, consumer_id, shop_id, goods_id, collect_time, is_cancel_collect, history_collect_cnt) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                generate_behavior_id(existing_collect_ids),  # collect_id
                consumer_id,
                shop_id,
                goods_id,
                behavior_datetime,
                random.choice([0, 1]),
                history_cnt
            )
            execute_insert(sql, data)

        elif behavior_type == '入会':
            membership_type = random.choice(['主动', '支付后自动入会'])
            sql = """
            INSERT INTO membership_info_detail 
            (membership_id, consumer_id, shop_id, join_time, membership_type, is_quit) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            data = (
                generate_behavior_id(existing_membership_ids),  # membership_id
                consumer_id,
                shop_id,
                behavior_datetime,
                membership_type,
                random.choice([0, 1])
            )
            execute_insert(sql, data)

        elif behavior_type == '咨询':
            goods_id = random.choice(goods_ids) if random.random() > 0.4 else None
            content = fake.sentence(nb_words=6) if random.random() > 0.1 else None
            is_replied = 1 if content and random.random() > 0.2 else 0
            reply_time = behavior_datetime + timedelta(seconds=random.randint(30, 300)) if is_replied else None
            sql = """
            INSERT INTO consult_info_detail 
            (consult_id, consumer_id, shop_id, goods_id, consult_time, consult_content, is_replied, reply_time) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                generate_behavior_id(existing_consult_ids),  # consult_id
                consumer_id,
                shop_id,
                goods_id,
                behavior_datetime,
                content,
                is_replied,
                reply_time
            )
            execute_insert(sql, data)

        elif behavior_type == '支付':
            # 支付行为会拆分成多条记录（一个订单多个商品）
            order_id = str(generate_unique_5digit_id(set()))  # 订单ID也用5位数
            goods_id = random.choice(goods_ids)
            sku_id = f"SKU{random.randint(1000, 9999)}"
            order_type = random.choice(order_types)
            is_pre_sale = 1 if order_type == '预售' else 0
            is_balance_paid = 1 if is_pre_sale and random.random() > 0.3 else 0  # 70%的预售订单付了尾款

            # 只有付了尾款或不是预售的订单才计入支付金额和买家数
            if is_balance_paid or not is_pre_sale:
                total_pay_amt = round(random.uniform(50, 1000), 2)
                freight_amt = round(random.uniform(0, 20), 2)
                shopping_fund_amt = round(random.uniform(0, 50), 2)
                red_packet_amt = round(random.uniform(0, 30), 2)
                deposit_amt = round(total_pay_amt * 0.2, 2) if is_pre_sale else 0.00
                balance_amt = total_pay_amt - deposit_amt if is_pre_sale else total_pay_amt

                # 根据文档要求，排除特定订单
                is_subsidy_half_hosting = 0
                is_official_bid = 0
                is_taote = 0
                is_cross_border = 0
                is_small_pay = 0

                # 随机生成少量退款
                refund_mid_sale = round(total_pay_amt * 0.1, 2) if random.random() < 0.05 else 0.00
                refund_after_sale = round(total_pay_amt * 0.15, 2) if random.random() < 0.03 else 0.00
                is_instant_refund = 1 if (refund_mid_sale > 0 or refund_after_sale > 0) and random.random() < 0.5 else 0

                # 优惠券不计入支付金额
                coupon_amt = round(random.uniform(5, 50), 2)

                sql = """
                INSERT INTO payment_info_detail 
                (payment_id, order_id, consumer_id, shop_id, goods_id, sku_id, payment_date, payment_time, 
                order_type, deposit_amt, balance_amt, total_pay_amt, freight_amt, shopping_fund_amt, 
                red_packet_amt, is_taote, is_cross_border, is_subsidy_half_hosting, is_official_bid, 
                coupon_amt, is_small_pay, refund_mid_sale, refund_after_sale, is_instant_refund, is_balance_paid) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                data = (
                    generate_behavior_id(existing_payment_ids),  # payment_id
                    order_id,
                    consumer_id,
                    shop_id,
                    goods_id,
                    sku_id,
                    behavior_date,
                    behavior_datetime,
                    order_type,
                    deposit_amt,
                    balance_amt,
                    total_pay_amt,
                    freight_amt,
                    shopping_fund_amt,
                    red_packet_amt,
                    is_taote,
                    is_cross_border,
                    is_subsidy_half_hosting,
                    is_official_bid,
                    coupon_amt,
                    is_small_pay,
                    refund_mid_sale,
                    refund_after_sale,
                    is_instant_refund,
                    is_balance_paid
                )
                execute_insert(sql, data)

        # 每插入1000条打印一次进度
        if (i + 1) % 1000 == 0:
            print(f"已生成 {i + 1} 条数据...")

    print("行为数据生成完成。")

def generate_broadcast_data():
    """生成人气播报数据 (基于已有的行为数据)"""
    print("正在生成人气播报数据...")
    behaviors = ['访问', '搜索', '加购', '收藏', '支付', '入会', '咨询']
    consumer_types = ['新客', '老客']
    channels = ['手淘搜索', '直通车']
    goods_ids = [random.randint(10000, 99999) for _ in range(50)]
    shop_ids = [random.randint(10000, 99999) for _ in range(5)]

    def generate_consumer_name():
        return fake.name()

    for i in range(500):  # 生成500条播报
        broadcast_id = random.randint(10000, 99999)
        minute_of_day = random.randint(0, 24*60 - 1)
        broadcast_datetime = datetime.strptime(TARGET_DATE, '%Y-%m-%d') + timedelta(minutes=minute_of_day)

        sql = """
        INSERT INTO popularity_broadcastinfo_detail 
        (broadcast_id, broadcast_time, shop_id, consumer_id, consumer_type, behavior_type, 
        channel, goods_id, history_behavior_cnt, broadcast_content, is_triggered) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = (
            broadcast_id,
            broadcast_datetime,
            random.choice(shop_ids),
            random.randint(10000, 99999),  # consumer_id
            random.choice(consumer_types),
            random.choice(behaviors),
            random.choice(channels),
            random.choice(goods_ids),
            random.randint(1, 10),
            f"顾客{generate_consumer_name()}通过{random.choice(channels)}{random.choice(behaviors)}了商品。",
            1
        )
        execute_insert(sql, data)

    print("人气播报数据生成完成。")

def generate_log_data():
    """生成日志表数据，数据来源为指定的日志文件"""
    print("正在生成日志数据...")
    log_content_lines = []

    # 读取日志文件
    try:
        with open(LOG_FILE_PATH, 'r', encoding='utf-8') as file:
            log_content_lines = file.readlines()
    except FileNotFoundError:
        print(f"日志文件 {LOG_FILE_PATH} 未找到，将生成默认日志。")
        # 如果文件不存在，则生成默认日志
        for i in range(100):
            log_content_lines.append(f"[{TARGET_DATE}] INFO: System processed {random.randint(100, 1000)} records successfully.\n")

    # 插入日志数据，id由数据库自增
    for line in log_content_lines:
        # 去除行尾的换行符
        log_content = line.strip()
        if log_content:  # 确保不是空行
            sql = "INSERT INTO z_log (log) VALUES (%s)"
            execute_insert(sql, (log_content,))

    print("日志数据生成完成。")

# ============= 主程序 =============
if __name__ == "__main__":
    try:
        # 1. 生成基础维度数据
        shop_ids, goods_ids, consumer_ids = generate_base_data()

        # 2. 生成行为事实数据
        generate_behavior_data(shop_ids, goods_ids, consumer_ids)

        # 3. 生成人气播报数据
        generate_broadcast_data()

        # 4. 生成日志数据
        generate_log_data()

        print("所有数据生成并插入完成！")

    except Exception as e:
        print(f"脚本执行出错: {e}")
    finally:
        cursor.close()
        connection.close()