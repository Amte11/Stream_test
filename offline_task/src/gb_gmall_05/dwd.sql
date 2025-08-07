CREATE DATABASE IF NOT EXISTS gmall_05;
USE gmall_05;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;


-- 1. 用户行为详情表（dwd_user_behavior_detail）
CREATE TABLE IF NOT EXISTS dwd_user_behavior_detail (
                                                        user_id         STRING      COMMENT '用户ID',
                                                        session_id      STRING      COMMENT '会话ID（同次访问标识）',
                                                        goods_id        STRING      COMMENT '商品ID',
                                                        refer_goods_id  STRING      COMMENT '前序访问商品ID',
                                                        behavior_type   STRING      COMMENT '行为类型：view-访问, favor-收藏, cart-加购',
                                                        behavior_time   TIMESTAMP   COMMENT '行为发生时间',
                                                        stay_duration   INT         COMMENT '页面停留时长(秒，非负)'
)
    COMMENT '用户行为详情表，清洗后用于分析商品关联访问/收藏行为'
    PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dwd/dwd_user_behavior_detail/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据清洗与插入
INSERT OVERWRITE TABLE dwd_user_behavior_detail PARTITION (dt = '2025-08-07')
SELECT
    user_id,
    session_id,
    goods_id,
    CASE
        WHEN refer_goods_id = '' OR refer_goods_id = 'null' THEN NULL
        ELSE refer_goods_id
        END AS refer_goods_id,
    CASE
        WHEN behavior_type IN ('view', 'favor', 'cart') THEN behavior_type
        ELSE 'invalid'
        END AS behavior_type,
    behavior_time,
    CASE
        WHEN stay_duration BETWEEN 0 AND 3600 THEN stay_duration  -- 0-60分钟
        WHEN stay_duration > 3600 THEN 3600  -- 超过1小时设为最大值
        ELSE 0  -- 负值设为0
        END AS stay_duration
FROM ods_user_behavior_log
WHERE dt = '2025-08-07';

select * from dwd_user_behavior_detail;


-- 2. 订单商品详情表（dwd_order_goods_detail）
CREATE TABLE IF NOT EXISTS dwd_order_goods_detail (
                                                      order_id        STRING      COMMENT '订单ID',
                                                      user_id         STRING      COMMENT '用户ID',
                                                      shop_id         STRING      COMMENT '店铺ID',
                                                      goods_id        STRING      COMMENT '商品ID',
                                                      goods_quantity  INT         COMMENT '购买数量（正整数）',
                                                      unit_price      DECIMAL(10,2) COMMENT '商品单价',
    pay_time        TIMESTAMP   COMMENT '支付时间',
    promotion_info  STRING      COMMENT '促销信息（清洗后）'
    )
    COMMENT '订单商品详情表，用于分析商品同时购买关联关系'
    PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dwd/dwd_order_goods_detail/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据清洗与插入（关联订单交易表获取支付信息）
INSERT OVERWRITE TABLE dwd_order_goods_detail PARTITION (dt = '2025-08-07')
SELECT
    og.detail_id AS order_id,
    ot.user_id,
    ot.shop_id,
    og.goods_id,
    CASE
        WHEN og.goods_quantity BETWEEN 1 AND 100 THEN og.goods_quantity
        ELSE 1
        END AS goods_quantity,
    CAST(ABS(og.unit_price) AS DECIMAL(10,2)) AS unit_price,
    ot.pay_time,
    CASE
        WHEN og.promotion_info = '' OR og.promotion_info IS NULL THEN '无促销'
        ELSE og.promotion_info
        END AS promotion_info
FROM ods_order_goods_detail og
         JOIN ods_order_trade_fact ot
              ON og.order_id = ot.order_id
                  AND og.dt = ot.dt
WHERE og.dt = '2025-08-07';

select * from dwd_order_goods_detail;

-- 3. 商品核心信息表（dwd_goods_core_info）
CREATE TABLE IF NOT EXISTS dwd_goods_core_info (
                                                   goods_id        STRING      COMMENT '商品ID',
                                                   shop_id         STRING      COMMENT '店铺ID',
                                                   goods_name      STRING      COMMENT '商品名称',
                                                   category_l1     STRING      COMMENT '一级类目',
                                                   category_l2     STRING      COMMENT '二级类目',
                                                   category_l3     STRING      COMMENT '三级类目',
                                                   is_traffic_item TINYINT     COMMENT '是否引流品(1是0否)',
                                                   is_top_seller   TINYINT     COMMENT '是否热销品(1是0否)',
                                                   base_price      DECIMAL(10,2) COMMENT '基础售价',
    update_time     TIMESTAMP   COMMENT '最后更新时间'
    )
    COMMENT '商品核心信息表，用于识别引流品/热销品主商品'
    PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dwd/dwd_goods_core_info/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据清洗与插入
INSERT OVERWRITE TABLE dwd_goods_core_info PARTITION (dt = '2025-08-07')
SELECT
    goods_id,
    shop_id,
    regexp_replace(trim(goods_name), '[\\x00-\\x1F\\x7F]', '') AS goods_name,
    NULLIF(category_l1, '') AS category_l1,
    NULLIF(category_l2, '') AS category_l2,
    NULLIF(category_l3, '') AS category_l3,
    CASE
        WHEN is_traffic_item = 1 THEN 1
        ELSE 0
        END AS is_traffic_item,
    CASE
        WHEN is_top_seller = 1 THEN 1
        ELSE 0
        END AS is_top_seller,
    CAST(ABS(base_price) AS DECIMAL(10,2)) AS base_price,
    update_time
FROM ods_goods_core_attr
WHERE dt = '2025-08-07';

select * from dwd_goods_core_info;



-- 4. 店铺基础信息表（dwd_shop_base_info）
CREATE TABLE IF NOT EXISTS dwd_shop_base_info (
                                                  shop_id         STRING      COMMENT '店铺ID',
                                                  shop_name       STRING      COMMENT '店铺名称',
                                                  shop_type       STRING      COMMENT '店铺类型',
                                                  main_category   STRING      COMMENT '主营类目'
)
    COMMENT '店铺基础信息表，用于限定商品所属店铺范围'
    PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dwd/dwd_shop_base_info/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据清洗与插入
INSERT OVERWRITE TABLE dwd_shop_base_info PARTITION (dt = '2025-08-07')
SELECT
    shop_id,
    regexp_replace(shop_name, '[\\x00-\\x1F]', '') AS shop_name,
    COALESCE(NULLIF(shop_type, ''), '其他') AS shop_type,
    COALESCE(NULLIF(main_category, ''), '综合') AS main_category
FROM ods_shop_base_info
WHERE dt = '2025-08-07';

select * from dwd_shop_base_info;


-- 5. 用户基础信息表（dwd_user_base_info）
CREATE TABLE IF NOT EXISTS dwd_user_base_info (
                                                  user_id         STRING      COMMENT '用户ID',
                                                  user_level      TINYINT     COMMENT '用户等级',
                                                  reg_channel     STRING      COMMENT '注册渠道'
)
    COMMENT '用户基础信息表，用于用户维度的商品关联分析'
    PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dwd/dwd_user_base_info/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据清洗与插入
INSERT OVERWRITE TABLE dwd_user_base_info PARTITION (dt = '2025-08-07')
SELECT
    user_id,
    CASE
        WHEN user_level BETWEEN 1 AND 5 THEN user_level
        ELSE 3
        END AS user_level,
    CASE
        WHEN lower(reg_channel) LIKE '%app%' THEN 'app'
        WHEN lower(reg_channel) LIKE '%web%' THEN 'web'
        ELSE 'other'
        END AS reg_channel
FROM ods_user_base_info
WHERE dt = '2025-08-07';

select * from dwd_user_base_info;