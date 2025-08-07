-- 创建 DWS 层表
CREATE DATABASE IF NOT EXISTS gmall_05;
USE gmall_05;

SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

-- 1. 用户行为聚合表（不同时间维度）
-- 日聚合
CREATE TABLE IF NOT EXISTS dws_user_behavior_agg_1d (
                                                        user_id         STRING      COMMENT '用户ID',
                                                        behavior_date   DATE        COMMENT '行为日期',
                                                        view_count      BIGINT      COMMENT '浏览次数',
                                                        favor_count     BIGINT      COMMENT '收藏次数',
                                                        cart_count      BIGINT      COMMENT '加购次数',
                                                        avg_stay_duration DOUBLE    COMMENT '平均停留时长(秒)',
                                                        distinct_goods_count BIGINT COMMENT '浏览商品数',
                                                        distinct_session_count BIGINT COMMENT '会话数'
)
    COMMENT '用户行为日聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_user_behavior_agg_1d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 1. 用户行为聚合表（1d）
INSERT OVERWRITE TABLE dws_user_behavior_agg_1d PARTITION (dt='2025-08-07')
SELECT
    ubl.user_id,
    DATE(ubl.behavior_time) AS behavior_date,
    SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ubl.behavior_type = 'favor' THEN 1 ELSE 0 END) AS favor_count,
    SUM(CASE WHEN ubl.behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
    AVG(ubl.stay_duration) AS avg_stay_duration,
    COUNT(DISTINCT ubl.goods_id) AS distinct_goods_count,
    COUNT(DISTINCT ubl.session_id) AS distinct_session_count
FROM ods_user_behavior_log ubl
WHERE ubl.dt = '2025-08-07'  -- 当天数据
GROUP BY ubl.user_id, DATE(ubl.behavior_time);

select * from dws_user_behavior_agg_1d;

-- 周聚合
CREATE TABLE IF NOT EXISTS dws_user_behavior_agg_7d (
                                                        user_id         STRING      COMMENT '用户ID',
                                                        view_count      BIGINT      COMMENT '浏览次数',
                                                        favor_count     BIGINT      COMMENT '收藏次数',
                                                        cart_count      BIGINT      COMMENT '加购次数',
                                                        avg_stay_duration DOUBLE    COMMENT '平均停留时长(秒)',
                                                        distinct_goods_count BIGINT COMMENT '浏览商品数',
                                                        distinct_session_count BIGINT COMMENT '会话数'
)
    COMMENT '用户行为周聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_user_behavior_agg_7d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 用户行为聚合表（7d）
INSERT OVERWRITE TABLE dws_user_behavior_agg_7d PARTITION (dt='2025-08-07')
SELECT
    ubl.user_id,
    SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ubl.behavior_type = 'favor' THEN 1 ELSE 0 END) AS favor_count,
    SUM(CASE WHEN ubl.behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
    AVG(ubl.stay_duration) AS avg_stay_duration,
    COUNT(DISTINCT ubl.goods_id) AS distinct_goods_count,
    COUNT(DISTINCT ubl.session_id) AS distinct_session_count
FROM ods_user_behavior_log ubl
WHERE ubl.dt BETWEEN '2025-08-01' AND '2025-08-07'  -- 7天数据
GROUP BY ubl.user_id;

select * from dws_user_behavior_agg_7d;

-- 月聚合
CREATE TABLE IF NOT EXISTS dws_user_behavior_agg_30d (
                                                         user_id         STRING      COMMENT '用户ID',
                                                         view_count      BIGINT      COMMENT '浏览次数',
                                                         favor_count     BIGINT      COMMENT '收藏次数',
                                                         cart_count      BIGINT      COMMENT '加购次数',
                                                         avg_stay_duration DOUBLE    COMMENT '平均停留时长(秒)',
                                                         distinct_goods_count BIGINT COMMENT '浏览商品数',
                                                         distinct_session_count BIGINT COMMENT '会话数'
)
    COMMENT '用户行为月聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_user_behavior_agg_30d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 用户行为聚合表（30d）
INSERT OVERWRITE TABLE dws_user_behavior_agg_30d PARTITION (dt='2025-08-07')
SELECT
    ubl.user_id,
    SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ubl.behavior_type = 'favor' THEN 1 ELSE 0 END) AS favor_count,
    SUM(CASE WHEN ubl.behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
    AVG(ubl.stay_duration) AS avg_stay_duration,
    COUNT(DISTINCT ubl.goods_id) AS distinct_goods_count,
    COUNT(DISTINCT ubl.session_id) AS distinct_session_count
FROM ods_user_behavior_log ubl
WHERE ubl.dt BETWEEN '2025-07-08' AND '2025-08-07'  -- 30天数据
GROUP BY ubl.user_id;

select * from dws_user_behavior_agg_30d;

-- 2. 商品销售聚合表（不同时间维度）
-- 日聚合
CREATE TABLE IF NOT EXISTS dws_goods_sales_agg_1d (
                                                      goods_id        STRING      COMMENT '商品ID',
                                                      goods_name      STRING      COMMENT '商品名称',
                                                      category_l1     STRING      COMMENT '一级类目',
                                                      category_l2     STRING      COMMENT '二级类目',
                                                      category_l3     STRING      COMMENT '三级类目',
                                                      total_sales     BIGINT      COMMENT '总销量',
                                                      total_revenue   DECIMAL(16,2) COMMENT '总销售额',
    avg_price       DECIMAL(10,2) COMMENT '平均售价',
    distinct_buyers BIGINT      COMMENT '购买用户数'
    )
    COMMENT '商品销售日聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_goods_sales_agg_1d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 商品销售聚合表（1d）
INSERT OVERWRITE TABLE dws_goods_sales_agg_1d PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3,
    SUM(ogd.goods_quantity) AS total_sales,
    SUM(ogd.total_price) AS total_revenue,
    AVG(ogd.unit_price) AS avg_price,
    COUNT(DISTINCT otf.user_id) AS distinct_buyers
FROM ods_order_goods_detail ogd
         JOIN ods_order_trade_fact otf
              ON ogd.order_id = otf.order_id
                  AND ogd.dt = otf.dt
         JOIN ods_goods_core_attr gca
              ON ogd.goods_id = gca.goods_id
                  AND ogd.dt = gca.dt
WHERE ogd.dt = '2025-08-07'  -- 当天数据
  AND otf.pay_status = 1  -- 只统计支付成功的订单
  AND gca.goods_status = 1  -- 只统计上架商品
GROUP BY
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3;



-- 周聚合
CREATE TABLE IF NOT EXISTS dws_goods_sales_agg_7d (
                                                      goods_id        STRING      COMMENT '商品ID',
                                                      goods_name      STRING      COMMENT '商品名称',
                                                      category_l1     STRING      COMMENT '一级类目',
                                                      category_l2     STRING      COMMENT '二级类目',
                                                      category_l3     STRING      COMMENT '三级类目',
                                                      total_sales     BIGINT      COMMENT '总销量',
                                                      total_revenue   DECIMAL(16,2) COMMENT '总销售额',
    avg_price       DECIMAL(10,2) COMMENT '平均售价',
    distinct_buyers BIGINT      COMMENT '购买用户数'
    )
    COMMENT '商品销售周聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_goods_sales_agg_7d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 商品销售聚合表（7d）
INSERT OVERWRITE TABLE dws_goods_sales_agg_7d PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3,
    SUM(ogd.goods_quantity) AS total_sales,
    SUM(ogd.total_price) AS total_revenue,
    AVG(ogd.unit_price) AS avg_price,
    COUNT(DISTINCT otf.user_id) AS distinct_buyers
FROM ods_order_goods_detail ogd
         JOIN ods_order_trade_fact otf
              ON ogd.order_id = otf.order_id
                  AND ogd.dt = otf.dt
         JOIN ods_goods_core_attr gca
              ON ogd.goods_id = gca.goods_id
                  AND ogd.dt = gca.dt
WHERE ogd.dt BETWEEN '2025-08-01' AND '2025-08-07'  -- 7天数据
  AND otf.pay_status = 1  -- 只统计支付成功的订单
  AND gca.goods_status = 1  -- 只统计上架商品
GROUP BY
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3;



-- 月聚合
CREATE TABLE IF NOT EXISTS dws_goods_sales_agg_30d (
                                                       goods_id        STRING      COMMENT '商品ID',
                                                       goods_name      STRING      COMMENT '商品名称',
                                                       category_l1     STRING      COMMENT '一级类目',
                                                       category_l2     STRING      COMMENT '二级类目',
                                                       category_l3     STRING      COMMENT '三级类目',
                                                       total_sales     BIGINT      COMMENT '总销量',
                                                       total_revenue   DECIMAL(16,2) COMMENT '总销售额',
    avg_price       DECIMAL(10,2) COMMENT '平均售价',
    distinct_buyers BIGINT      COMMENT '购买用户数'
    )
    COMMENT '商品销售月聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_goods_sales_agg_30d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 商品销售聚合表（30d）
INSERT OVERWRITE TABLE dws_goods_sales_agg_30d PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3,
    SUM(ogd.goods_quantity) AS total_sales,
    SUM(ogd.total_price) AS total_revenue,
    AVG(ogd.unit_price) AS avg_price,
    COUNT(DISTINCT otf.user_id) AS distinct_buyers
FROM ods_order_goods_detail ogd
         JOIN ods_order_trade_fact otf
              ON ogd.order_id = otf.order_id
                  AND ogd.dt = otf.dt
         JOIN ods_goods_core_attr gca
              ON ogd.goods_id = gca.goods_id
                  AND ogd.dt = gca.dt
WHERE ogd.dt BETWEEN '2025-07-08' AND '2025-08-07'  -- 30天数据
  AND otf.pay_status = 1  -- 只统计支付成功的订单
  AND gca.goods_status = 1  -- 只统计上架商品
GROUP BY
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3;

select * from dws_goods_sales_agg_30d;



-- 3. 店铺销售聚合表（不同时间维度）
-- 日聚合
CREATE TABLE IF NOT EXISTS dws_shop_sales_agg_1d (
                                                     shop_id         STRING      COMMENT '店铺ID',
                                                     shop_name       STRING      COMMENT '店铺名称',
                                                     shop_level      TINYINT     COMMENT '店铺等级',
                                                     shop_type       STRING      COMMENT '店铺类型',
                                                     main_category   STRING      COMMENT '主营类目',
                                                     total_orders    BIGINT      COMMENT '总订单数',
                                                     total_sales     BIGINT      COMMENT '总销量',
                                                     total_revenue   DECIMAL(16,2) COMMENT '总销售额',
    avg_order_value DECIMAL(10,2) COMMENT '客单价',
    distinct_buyers BIGINT      COMMENT '购买用户数'
    )
    COMMENT '店铺销售日聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_shop_sales_agg_1d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 3. 店铺销售聚合表（1d）
INSERT OVERWRITE TABLE dws_shop_sales_agg_1d PARTITION (dt='2025-08-07')
SELECT
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category,
    COUNT(DISTINCT otf.order_id) AS total_orders,
    SUM(ogd.goods_quantity) AS total_sales,
    SUM(ogd.total_price) AS total_revenue,
    AVG(otf.payment_amount) AS avg_order_value,
    COUNT(DISTINCT otf.user_id) AS distinct_buyers
FROM ods_order_trade_fact otf
         JOIN ods_order_goods_detail ogd
              ON otf.order_id = ogd.order_id
                  AND otf.dt = ogd.dt
         JOIN ods_shop_base_info sbi
              ON otf.shop_id = sbi.shop_id
                  AND otf.dt = sbi.dt
WHERE otf.dt = '2025-08-07'  -- 当天数据
  AND otf.pay_status = 1  -- 只统计支付成功的订单
GROUP BY
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category;

select * from dws_shop_sales_agg_1d;

-- 周聚合
CREATE TABLE IF NOT EXISTS dws_shop_sales_agg_7d (
                                                     shop_id         STRING      COMMENT '店铺ID',
                                                     shop_name       STRING      COMMENT '店铺名称',
                                                     shop_level      TINYINT     COMMENT '店铺等级',
                                                     shop_type       STRING      COMMENT '店铺类型',
                                                     main_category   STRING      COMMENT '主营类目',
                                                     total_orders    BIGINT      COMMENT '总订单数',
                                                     total_sales     BIGINT      COMMENT '总销量',
                                                     total_revenue   DECIMAL(16,2) COMMENT '总销售额',
    avg_order_value DECIMAL(10,2) COMMENT '客单价',
    distinct_buyers BIGINT      COMMENT '购买用户数'
    )
    COMMENT '店铺销售周聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_shop_sales_agg_7d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 店铺销售聚合表（7d）
INSERT OVERWRITE TABLE dws_shop_sales_agg_7d PARTITION (dt='2025-08-07')
SELECT
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category,
    COUNT(DISTINCT otf.order_id) AS total_orders,
    SUM(ogd.goods_quantity) AS total_sales,
    SUM(ogd.total_price) AS total_revenue,
    AVG(otf.payment_amount) AS avg_order_value,
    COUNT(DISTINCT otf.user_id) AS distinct_buyers
FROM ods_order_trade_fact otf
         JOIN ods_order_goods_detail ogd
              ON otf.order_id = ogd.order_id
                  AND otf.dt = ogd.dt
         JOIN ods_shop_base_info sbi
              ON otf.shop_id = sbi.shop_id
                  AND otf.dt = sbi.dt
WHERE otf.dt BETWEEN '2025-08-01' AND '2025-08-07'  -- 7天数据
  AND otf.pay_status = 1  -- 只统计支付成功的订单
GROUP BY
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category;

select * from dws_shop_sales_agg_7d;

-- 月聚合
CREATE TABLE IF NOT EXISTS dws_shop_sales_agg_30d (
                                                      shop_id         STRING      COMMENT '店铺ID',
                                                      shop_name       STRING      COMMENT '店铺名称',
                                                      shop_level      TINYINT     COMMENT '店铺等级',
                                                      shop_type       STRING      COMMENT '店铺类型',
                                                      main_category   STRING      COMMENT '主营类目',
                                                      total_orders    BIGINT      COMMENT '总订单数',
                                                      total_sales     BIGINT      COMMENT '总销量',
                                                      total_revenue   DECIMAL(16,2) COMMENT '总销售额',
    avg_order_value DECIMAL(10,2) COMMENT '客单价',
    distinct_buyers BIGINT      COMMENT '购买用户数'
    )
    COMMENT '店铺销售月聚合表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_shop_sales_agg_30d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 店铺销售聚合表（30d）
INSERT OVERWRITE TABLE dws_shop_sales_agg_30d PARTITION (dt='2025-08-07')
SELECT
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category,
    COUNT(DISTINCT otf.order_id) AS total_orders,
    SUM(ogd.goods_quantity) AS total_sales,
    SUM(ogd.total_price) AS total_revenue,
    AVG(otf.payment_amount) AS avg_order_value,
    COUNT(DISTINCT otf.user_id) AS distinct_buyers
FROM ods_order_trade_fact otf
         JOIN ods_order_goods_detail ogd
              ON otf.order_id = ogd.order_id
                  AND otf.dt = ogd.dt
         JOIN ods_shop_base_info sbi
              ON otf.shop_id = sbi.shop_id
                  AND otf.dt = sbi.dt
WHERE otf.dt BETWEEN '2025-07-08' AND '2025-08-07'  -- 30天数据
  AND otf.pay_status = 1  -- 只统计支付成功的订单
GROUP BY
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category;

select * from dws_shop_sales_agg_30d;

-- 4. 用户购买行为表（不同时间维度）
-- 日聚合
CREATE TABLE IF NOT EXISTS dws_user_purchase_behavior_1d (
                                                             user_id         STRING      COMMENT '用户ID',
                                                             user_name       STRING      COMMENT '用户名',
                                                             user_level      TINYINT     COMMENT '用户等级',
                                                             total_orders    BIGINT      COMMENT '总订单数',
                                                             total_spent     DECIMAL(16,2) COMMENT '总消费金额',
    avg_order_value DECIMAL(10,2) COMMENT '客单价',
    last_purchase_date DATE     COMMENT '最后购买日期',
    favorite_category STRING    COMMENT '最常购买类目'
    )
    COMMENT '用户购买行为日表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_user_purchase_behavior_1d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 4. 用户购买行为表（1d）
INSERT OVERWRITE TABLE dws_user_purchase_behavior_1d PARTITION (dt='2025-08-07')
SELECT
    ubi.user_id,
    ubi.user_name,
    ubi.user_level,
    COUNT(DISTINCT otf.order_id) AS total_orders,
    SUM(otf.payment_amount) AS total_spent,
    AVG(otf.payment_amount) AS avg_order_value,
    MAX(DATE(otf.pay_time)) AS last_purchase_date,
    MAX(gca.category_l1) AS favorite_category
FROM ods_order_trade_fact otf
         LEFT JOIN ods_user_base_info ubi
                   ON otf.user_id = ubi.user_id AND otf.dt = ubi.dt
         LEFT JOIN ods_order_goods_detail ogd
                   ON otf.order_id = ogd.order_id AND otf.dt = ogd.dt
         LEFT JOIN ods_goods_core_attr gca
                   ON ogd.goods_id = gca.goods_id AND ogd.dt = gca.dt
WHERE otf.dt = '2025-08-07'
  AND otf.pay_status = 1 -- 当天数据 -- 只统计支付成功的订单
GROUP BY
    ubi.user_id,
    ubi.user_name,
    ubi.user_level;

select * from dws_user_purchase_behavior_1d;

-- 周聚合
CREATE TABLE IF NOT EXISTS dws_user_purchase_behavior_7d (
                                                             user_id         STRING      COMMENT '用户ID',
                                                             user_name       STRING      COMMENT '用户名',
                                                             user_level      TINYINT     COMMENT '用户等级',
                                                             total_orders    BIGINT      COMMENT '总订单数',
                                                             total_spent     DECIMAL(16,2) COMMENT '总消费金额',
    avg_order_value DECIMAL(10,2) COMMENT '客单价',
    last_purchase_date DATE     COMMENT '最后购买日期',
    favorite_category STRING    COMMENT '最常购买类目'
    )
    COMMENT '用户购买行为周表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_user_purchase_behavior_7d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 用户购买行为表（7d）
INSERT OVERWRITE TABLE dws_user_purchase_behavior_7d PARTITION (dt='2025-08-07')
SELECT
    ubi.user_id,
    ubi.user_name,
    ubi.user_level,
    COUNT(DISTINCT otf.order_id) AS total_orders,
    SUM(otf.payment_amount) AS total_spent,
    AVG(otf.payment_amount) AS avg_order_value,
    MAX(DATE(otf.pay_time)) AS last_purchase_date,
    MAX(gca.category_l1) AS favorite_category
FROM ods_order_trade_fact otf
         LEFT JOIN ods_user_base_info ubi
                   ON otf.user_id = ubi.user_id AND otf.dt = ubi.dt
         LEFT JOIN ods_order_goods_detail ogd
                   ON otf.order_id = ogd.order_id AND otf.dt = ogd.dt
         LEFT JOIN ods_goods_core_attr gca
                   ON ogd.goods_id = gca.goods_id AND ogd.dt = gca.dt
WHERE otf.dt BETWEEN '2025-08-01' AND '2025-08-07'
  AND otf.pay_status = 1-- 7天数据-- 只统计支付成功的订单
GROUP BY
    ubi.user_id,
    ubi.user_name,
    ubi.user_level;

select * from dws_user_purchase_behavior_7d;

-- 月聚合
CREATE TABLE IF NOT EXISTS dws_user_purchase_behavior_30d (
                                                              user_id         STRING      COMMENT '用户ID',
                                                              user_name       STRING      COMMENT '用户名',
                                                              user_level      TINYINT     COMMENT '用户等级',
                                                              total_orders    BIGINT      COMMENT '总订单数',
                                                              total_spent     DECIMAL(16,2) COMMENT '总消费金额',
    avg_order_value DECIMAL(10,2) COMMENT '客单价',
    last_purchase_date DATE     COMMENT '最后购买日期',
    favorite_category STRING    COMMENT '最常购买类目'
    )
    COMMENT '用户购买行为月表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_user_purchase_behavior_30d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 用户购买行为表（30d）
INSERT OVERWRITE TABLE dws_user_purchase_behavior_30d PARTITION (dt='2025-08-07')
SELECT
    ubi.user_id,
    ubi.user_name,
    ubi.user_level,
    COUNT(DISTINCT otf.order_id) AS total_orders,
    SUM(otf.payment_amount) AS total_spent,
    AVG(otf.payment_amount) AS avg_order_value,
    MAX(DATE(otf.pay_time)) AS last_purchase_date,
    MAX(gca.category_l1) AS favorite_category
FROM ods_order_trade_fact otf
         LEFT JOIN ods_user_base_info ubi
                   ON otf.user_id = ubi.user_id AND otf.dt = ubi.dt
         LEFT JOIN ods_order_goods_detail ogd
                   ON otf.order_id = ogd.order_id AND otf.dt = ogd.dt
         LEFT JOIN ods_goods_core_attr gca
                   ON ogd.goods_id = gca.goods_id AND ogd.dt = gca.dt
WHERE otf.dt BETWEEN '2025-07-07' AND '2025-08-07'
  AND otf.pay_status = 1  -- 30天数  -- 只统计支付成功的订单
GROUP BY
    ubi.user_id,
    ubi.user_name,
    ubi.user_level;

select * from dws_user_purchase_behavior_30d;

-- 5. 商品行为分析表（不同时间维度）
-- 日聚合
CREATE TABLE IF NOT EXISTS dws_goods_behavior_analysis_1d (
                                                              goods_id        STRING      COMMENT '商品ID',
                                                              goods_name      STRING      COMMENT '商品名称',
                                                              category_l1     STRING      COMMENT '一级类目',
                                                              category_l2     STRING      COMMENT '二级类目',
                                                              view_count      BIGINT      COMMENT '浏览次数',
                                                              favor_count     BIGINT      COMMENT '收藏次数',
                                                              cart_count      BIGINT      COMMENT '加购次数',
                                                              purchase_count  BIGINT      COMMENT '购买次数',
                                                              conversion_rate DOUBLE      COMMENT '转化率',
                                                              avg_stay_duration DOUBLE    COMMENT '平均停留时长(秒)'
)
    COMMENT '商品行为分析日表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_goods_behavior_analysis_1d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 5. 商品行为分析表（1d）
INSERT OVERWRITE TABLE dws_goods_behavior_analysis_1d PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ubl.behavior_type = 'favor' THEN 1 ELSE 0 END) AS favor_count,
    SUM(CASE WHEN ubl.behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
    COUNT(DISTINCT CASE WHEN otf.pay_status = 1 THEN otf.order_id END) AS purchase_count,
    COUNT(DISTINCT CASE WHEN otf.pay_status = 1 THEN otf.order_id END) /
    NULLIF(SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END), 0) AS conversion_rate,
    AVG(ubl.stay_duration) AS avg_stay_duration
FROM ods_goods_core_attr gca
         LEFT JOIN ods_user_behavior_log ubl
                   ON gca.goods_id = ubl.goods_id
                       AND gca.dt = ubl.dt
         LEFT JOIN (
    SELECT
        ogd.goods_id,
        otf.order_id,
        otf.pay_status
    FROM ods_order_goods_detail ogd
             JOIN ods_order_trade_fact otf
                  ON ogd.order_id = otf.order_id
                      AND ogd.dt = otf.dt
    WHERE otf.dt = '2025-08-07'
) otf ON gca.goods_id = otf.goods_id
WHERE gca.dt = '2025-08-07'  -- 当天数据
-- 只统计上架商品
GROUP BY
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2;

select * from dws_goods_behavior_analysis_1d;

-- 周聚合
CREATE TABLE IF NOT EXISTS dws_goods_behavior_analysis_7d (
                                                              goods_id        STRING      COMMENT '商品ID',
                                                              goods_name      STRING      COMMENT '商品名称',
                                                              category_l1     STRING      COMMENT '一级类目',
                                                              category_l2     STRING      COMMENT '二级类目',
                                                              view_count      BIGINT      COMMENT '浏览次数',
                                                              favor_count     BIGINT      COMMENT '收藏次数',
                                                              cart_count      BIGINT      COMMENT '加购次数',
                                                              purchase_count  BIGINT      COMMENT '购买次数',
                                                              conversion_rate DOUBLE      COMMENT '转化率',
                                                              avg_stay_duration DOUBLE    COMMENT '平均停留时长(秒)'
)
    COMMENT '商品行为分析周表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_goods_behavior_analysis_7d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 商品行为分析表（7d）
INSERT OVERWRITE TABLE dws_goods_behavior_analysis_7d PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ubl.behavior_type = 'favor' THEN 1 ELSE 0 END) AS favor_count,
    SUM(CASE WHEN ubl.behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
    COUNT(DISTINCT CASE WHEN otf.pay_status = 1 THEN otf.order_id END) AS purchase_count,
    COUNT(DISTINCT CASE WHEN otf.pay_status = 1 THEN otf.order_id END) /
    NULLIF(SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END), 0) AS conversion_rate,
    AVG(ubl.stay_duration) AS avg_stay_duration
FROM ods_goods_core_attr gca
         LEFT JOIN ods_user_behavior_log ubl
                   ON gca.goods_id = ubl.goods_id
                       AND gca.dt = ubl.dt
         LEFT JOIN (
    SELECT
        ogd.goods_id,
        otf.order_id,
        otf.pay_status
    FROM ods_order_goods_detail ogd
             JOIN ods_order_trade_fact otf
                  ON ogd.order_id = otf.order_id
                      AND ogd.dt = otf.dt
    WHERE otf.dt BETWEEN '2025-08-01' AND '2025-08-07'
) otf ON gca.goods_id = otf.goods_id
WHERE gca.dt BETWEEN '2025-08-01' AND '2025-08-07'  -- 7天数据-- 只统计上架商品
GROUP BY
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2;

select * from dws_goods_behavior_analysis_7d;

-- 月聚合
CREATE TABLE IF NOT EXISTS dws_goods_behavior_analysis_30d (
                                                               goods_id        STRING      COMMENT '商品ID',
                                                               goods_name      STRING      COMMENT '商品名称',
                                                               category_l1     STRING      COMMENT '一级类目',
                                                               category_l2     STRING      COMMENT '二级类目',
                                                               view_count      BIGINT      COMMENT '浏览次数',
                                                               favor_count     BIGINT      COMMENT '收藏次数',
                                                               cart_count      BIGINT      COMMENT '加购次数',
                                                               purchase_count  BIGINT      COMMENT '购买次数',
                                                               conversion_rate DOUBLE      COMMENT '转化率',
                                                               avg_stay_duration DOUBLE    COMMENT '平均停留时长(秒)'
)
    COMMENT '商品行为分析月表'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/dws/dws_goods_behavior_analysis_30d/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 商品行为分析表（30d）
INSERT OVERWRITE TABLE dws_goods_behavior_analysis_30d PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ubl.behavior_type = 'favor' THEN 1 ELSE 0 END) AS favor_count,
    SUM(CASE WHEN ubl.behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
    COUNT(DISTINCT CASE WHEN otf.pay_status = 1 THEN otf.order_id END) AS purchase_count,
    COUNT(DISTINCT CASE WHEN otf.pay_status = 1 THEN otf.order_id END) /
    NULLIF(SUM(CASE WHEN ubl.behavior_type = 'view' THEN 1 ELSE 0 END), 0) AS conversion_rate,
    AVG(ubl.stay_duration) AS avg_stay_duration
FROM ods_goods_core_attr gca
         LEFT JOIN ods_user_behavior_log ubl
                   ON gca.goods_id = ubl.goods_id
                       AND gca.dt = ubl.dt
         LEFT JOIN (
    SELECT
        ogd.goods_id,
        otf.order_id,
        otf.pay_status
    FROM ods_order_goods_detail ogd
             JOIN ods_order_trade_fact otf
                  ON ogd.order_id = otf.order_id
                      AND ogd.dt = otf.dt
    WHERE otf.dt BETWEEN '2025-07-08' AND '2025-08-07'
) otf ON gca.goods_id = otf.goods_id
WHERE gca.dt BETWEEN '2025-07-08' AND '2025-08-07'  -- 30天数据 -- 只统计上架商品
GROUP BY
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2;

select * from dws_goods_behavior_analysis_30d;