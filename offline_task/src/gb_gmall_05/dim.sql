CREATE DATABASE IF NOT EXISTS gmall_05;
USE gmall_05;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

-- 1. 商品维度拉链表 (添加分区)
CREATE TABLE IF NOT EXISTS dim_goods_zip (
                                             goods_id        STRING      COMMENT '商品ID',
                                             shop_id         STRING      COMMENT '店铺ID',
                                             goods_name      STRING      COMMENT '商品名称',
                                             category_l1     STRING      COMMENT '一级类目',
                                             category_l2     STRING      COMMENT '二级类目',
                                             category_l3     STRING      COMMENT '三级类目',
                                             is_traffic_item TINYINT     COMMENT '是否引流品(1是0否)',
                                             is_top_seller   TINYINT     COMMENT '是否热销品(1是0否)',
                                             base_price      DECIMAL(10,2) COMMENT '基础售价',
    goods_status    TINYINT     COMMENT '商品状态(1上架 0下架)',
    create_date     DATE        COMMENT '创建日期',
    update_date     DATE        COMMENT '最后更新日期',
    start_date      STRING      COMMENT '生效日期',
    end_date        STRING      COMMENT '失效日期'
    ) COMMENT '商品维度拉链表'
    PARTITIONED BY (dt STRING)  -- 添加分区字段
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dim/dim_goods_zip/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dim_goods_zip PARTITION (dt='2025-08-07')  -- 插入分区
SELECT
    goods_id,
    shop_id,
    COALESCE(NULLIF(goods_name, ''), CONCAT('商品_', goods_id)) AS goods_name,
    COALESCE(NULLIF(category_l1, ''), '未分类') AS category_l1,
    COALESCE(NULLIF(category_l2, ''), '未分类') AS category_l2,
    COALESCE(NULLIF(category_l3, ''), '未分类') AS category_l3,
    COALESCE(is_traffic_item, 0) AS is_traffic_item,
    COALESCE(is_top_seller, 0) AS is_top_seller,
    CAST(ABS(COALESCE(base_price, 0)) AS DECIMAL(10,2)) AS base_price,
    COALESCE(goods_status, 0) AS goods_status,
    TO_DATE(COALESCE(create_time, '2025-07-01')) AS create_date,
    TO_DATE(COALESCE(update_time, '2025-07-01')) AS update_date,
    '2025-08-07' AS start_date,
    '9999-12-31' AS end_date
FROM ods_goods_core_attr
WHERE dt = '2025-08-07';

select  * from dim_goods_zip;

-- 2. 店铺维度拉链表 (添加分区)
CREATE TABLE IF NOT EXISTS dim_shop_zip (
                                            shop_id         STRING      COMMENT '店铺ID',
                                            shop_name       STRING      COMMENT '店铺名称',
                                            seller_user_id  STRING      COMMENT '商家用户ID',
                                            shop_level      TINYINT     COMMENT '店铺等级(1-5星)',
                                            shop_type       STRING      COMMENT '店铺类型',
                                            main_category   STRING      COMMENT '主营类目',
                                            create_date     DATE        COMMENT '开店日期',
                                            start_date      STRING      COMMENT '生效日期',
                                            end_date        STRING      COMMENT '失效日期'
) COMMENT '店铺维度拉链表'
    PARTITIONED BY (dt STRING)  -- 添加分区字段
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dim/dim_shop_zip/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dim_shop_zip PARTITION (dt='2025-08-07')  -- 插入分区
SELECT
    shop_id,
    COALESCE(NULLIF(shop_name, ''), CONCAT('店铺_', shop_id)) AS shop_name,
    seller_user_id,
    CASE
        WHEN shop_level BETWEEN 1 AND 5 THEN shop_level
        ELSE 3
        END AS shop_level,
    COALESCE(NULLIF(shop_type, ''), '普通店') AS shop_type,
    COALESCE(NULLIF(main_category, ''), '综合') AS main_category,
    COALESCE(create_date, '2025-07-01') AS create_date,
    '2025-08-07' AS start_date,
    '9999-12-31' AS end_date
FROM gmall_05.ods_shop_base_info
WHERE dt = '2025-08-07';

select * from dim_shop_zip;

-- 3. 用户维度拉链表 (添加分区)
CREATE TABLE IF NOT EXISTS dim_user_zip (
                                            user_id         STRING      COMMENT '用户ID',
                                            user_name       STRING      COMMENT '用户名',
                                            reg_channel     STRING      COMMENT '注册渠道',
                                            user_level      TINYINT     COMMENT '用户等级',
                                            reg_date        DATE        COMMENT '注册日期',
                                            start_date      STRING      COMMENT '生效日期',
                                            end_date        STRING      COMMENT '失效日期'
) COMMENT '用户维度拉链表'
    PARTITIONED BY (dt STRING)  -- 添加分区字段
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dim_user_zip PARTITION (dt='2025-08-07')  -- 插入分区
SELECT
    user_id,
    COALESCE(NULLIF(user_name, ''), CONCAT('用户_', user_id)) AS user_name,
    COALESCE(NULLIF(reg_channel, ''), 'web') AS reg_channel,
    CASE
        WHEN user_level BETWEEN 1 AND 10 THEN user_level
        ELSE 1
        END AS user_level,
    COALESCE(reg_date, '2025-07-01') AS reg_date,
    '2025-08-07' AS start_date,
    '9999-12-31' AS end_date
FROM gmall_05.ods_user_base_info
WHERE dt = '2025-08-07';

select * from dim_user_zip;

-- 4. 支付渠道维度表 (添加分区)
CREATE TABLE IF NOT EXISTS dim_pay_channel_zip (
                                                   channel_code    STRING      COMMENT '渠道编码',
                                                   channel_name    STRING      COMMENT '渠道名称',
                                                   channel_type    STRING      COMMENT '渠道类型',
                                                   start_date      STRING      COMMENT '生效日期',
                                                   end_date        STRING      COMMENT '失效日期'
) COMMENT '支付渠道维度表'
    PARTITIONED BY (dt STRING)  -- 添加分区字段
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dim/dim_pay_channel_zip/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dim_pay_channel_zip PARTITION (dt='2025-08-07')  -- 插入分区
SELECT DISTINCT
    pay_channel AS channel_code,
    CASE pay_channel
        WHEN 'alipay' THEN '支付宝'
        WHEN 'wechat' THEN '微信支付'
        WHEN 'bank' THEN '银行卡支付'
        WHEN 'balance' THEN '余额支付'
        ELSE '其他支付'
        END AS channel_name,
    CASE pay_channel
        WHEN 'alipay' THEN '第三方支付'
        WHEN 'wechat' THEN '第三方支付'
        WHEN 'bank' THEN '银行支付'
        WHEN 'balance' THEN '账户支付'
        ELSE '其他'
        END AS channel_type,
    '2025-08-07' AS start_date,
    '9999-12-31' AS end_date
FROM gmall_05.ods_order_trade_fact
WHERE dt = '2025-08-07';

select * from dim_pay_channel_zip;

-- 5. 行为类型维度表 (添加分区)
CREATE TABLE IF NOT EXISTS dim_behavior_type_zip (
                                                     behavior_code   STRING      COMMENT '行为编码',
                                                     behavior_name   STRING      COMMENT '行为名称',
                                                     behavior_group  STRING      COMMENT '行为分组',
                                                     start_date      STRING      COMMENT '生效日期',
                                                     end_date        STRING      COMMENT '失效日期'
) COMMENT '行为类型维度表'
    PARTITIONED BY (dt STRING)  -- 添加分区字段
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dim/dim_behavior_type_zip/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dim_behavior_type_zip PARTITION (dt='2025-08-07')  -- 插入分区
SELECT DISTINCT
    behavior_type AS behavior_code,
    CASE behavior_type
        WHEN 'view' THEN '访问'
        WHEN 'favor' THEN '收藏'
        WHEN 'cart' THEN '加购'
        ELSE '其他行为'
        END AS behavior_name,
    CASE behavior_type
        WHEN 'view' THEN '浏览行为'
        WHEN 'favor' THEN '互动行为'
        WHEN 'cart' THEN '互动行为'
        ELSE '其他'
        END AS behavior_group,
    '2025-08-07' AS start_date,
    '9999-12-31' AS end_date
FROM gmall_05.ods_user_behavior_log
WHERE dt = '2025-08-07';

select * from dim_behavior_type_zip;

-- 6. 设备维度表 (添加分区)
CREATE TABLE IF NOT EXISTS dim_device_zip (
                                              device_id       STRING      COMMENT '设备ID',
                                              app_version     STRING      COMMENT 'APP版本号',
                                              device_type     STRING      COMMENT '设备类型',
                                              start_date      STRING      COMMENT '生效日期',
                                              end_date        STRING      COMMENT '失效日期'
) COMMENT '设备维度表'
    PARTITIONED BY (dt STRING)  -- 添加分区字段
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dim/dim_device_zip/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dim_device_zip PARTITION (dt='2025-08-07')  -- 插入分区
SELECT
    device_id,
    app_version,
    CASE
        WHEN device_id LIKE 'iOS%' THEN 'iOS'
        WHEN device_id LIKE 'Android%' THEN 'Android'
        WHEN device_id LIKE 'PC%' THEN 'PC'
        ELSE '其他设备'
        END AS device_type,
    '2025-08-07' AS start_date,
    '9999-12-31' AS end_date
FROM (
         SELECT device_id, app_version
         FROM gmall_05.ods_user_behavior_log
         WHERE dt = '2025-08-07'
         GROUP BY device_id, app_version
     ) t;

select * from dim_device_zip;