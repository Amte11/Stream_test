-- 创建ADS层数据库
CREATE DATABASE IF NOT EXISTS gmall_05;
USE gmall_05;

SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

-- 1. 用户行为汇总表
CREATE TABLE IF NOT EXISTS ads_user_behavior_summary (
                                                         user_id              STRING    COMMENT '用户ID',
                                                         user_name            STRING    COMMENT '用户名',
                                                         user_level           TINYINT   COMMENT '用户等级',
                                                         last_7d_view_count   BIGINT    COMMENT '近7日浏览次数',
                                                         last_7d_favor_count  BIGINT    COMMENT '近7日收藏次数',
                                                         last_7d_cart_count   BIGINT    COMMENT '近7日加购次数',
                                                         last_7d_avg_stay_duration DOUBLE COMMENT '近7日平均停留时长(秒)',
                                                         last_7d_distinct_goods BIGINT  COMMENT '近7日浏览商品数',
                                                         last_30d_view_count  BIGINT    COMMENT '近30日浏览次数',
                                                         last_30d_favor_count BIGINT    COMMENT '近30日收藏次数',
                                                         last_30d_cart_count  BIGINT    COMMENT '近30日加购次数',
                                                         last_30d_avg_stay_duration DOUBLE COMMENT '近30日平均停留时长(秒)',
                                                         last_30d_distinct_goods BIGINT COMMENT '近30日浏览商品数'
)
    COMMENT '用户行为汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/ads/ads_user_behavior_summary/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_user_behavior_summary PARTITION (dt='2025-08-07')
SELECT
    ubi.user_id,
    ubi.user_name,
    ubi.user_level,
    COALESCE(agg7d.view_count, 0) AS last_7d_view_count,
    COALESCE(agg7d.favor_count, 0) AS last_7d_favor_count,
    COALESCE(agg7d.cart_count, 0) AS last_7d_cart_count,
    COALESCE(agg7d.avg_stay_duration, 0) AS last_7d_avg_stay_duration,
    COALESCE(agg7d.distinct_goods_count, 0) AS last_7d_distinct_goods,
    COALESCE(agg30d.view_count, 0) AS last_30d_view_count,
    COALESCE(agg30d.favor_count, 0) AS last_30d_favor_count,
    COALESCE(agg30d.cart_count, 0) AS last_30d_cart_count,
    COALESCE(agg30d.avg_stay_duration, 0) AS last_30d_avg_stay_duration,
    COALESCE(agg30d.distinct_goods_count, 0) AS last_30d_distinct_goods
FROM ods_user_base_info ubi
         LEFT JOIN dws_user_behavior_agg_7d agg7d
                   ON ubi.user_id = agg7d.user_id AND agg7d.dt = '2025-08-07'
         LEFT JOIN dws_user_behavior_agg_30d agg30d
                   ON ubi.user_id = agg30d.user_id AND agg30d.dt = '2025-08-07'
WHERE ubi.dt = '2025-08-07';

SELECT * FROM ads_user_behavior_summary;

-- 2. 商品销售汇总表
CREATE TABLE IF NOT EXISTS ads_goods_sales_summary (
                                                       goods_id          STRING      COMMENT '商品ID',
                                                       goods_name        STRING      COMMENT '商品名称',
                                                       category_l1       STRING      COMMENT '一级类目',
                                                       category_l2       STRING      COMMENT '二级类目',
                                                       category_l3       STRING      COMMENT '三级类目',
                                                       last_7d_sales     BIGINT      COMMENT '近7日销量',
                                                       last_7d_revenue   DECIMAL(16,2) COMMENT '近7日销售额',
    last_7d_avg_price DECIMAL(10,2) COMMENT '近7日平均售价',
    last_7d_buyers    BIGINT      COMMENT '近7日购买用户数',
    last_30d_sales    BIGINT      COMMENT '近30日销量',
    last_30d_revenue  DECIMAL(16,2) COMMENT '近30日销售额',
    last_30d_avg_price DECIMAL(10,2) COMMENT '近30日平均售价',
    last_30d_buyers   BIGINT      COMMENT '近30日购买用户数'
    )
    COMMENT '商品销售汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/ads/ads_goods_sales_summary/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_goods_sales_summary PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    gca.category_l3,
    COALESCE(agg7d.total_sales, 0) AS last_7d_sales,
    COALESCE(agg7d.total_revenue, 0) AS last_7d_revenue,
    COALESCE(agg7d.avg_price, 0) AS last_7d_avg_price,
    COALESCE(agg7d.distinct_buyers, 0) AS last_7d_buyers,
    COALESCE(agg30d.total_sales, 0) AS last_30d_sales,
    COALESCE(agg30d.total_revenue, 0) AS last_30d_revenue,
    COALESCE(agg30d.avg_price, 0) AS last_30d_avg_price,
    COALESCE(agg30d.distinct_buyers, 0) AS last_30d_buyers
FROM ods_goods_core_attr gca
         LEFT JOIN dws_goods_sales_agg_7d agg7d
                   ON gca.goods_id = agg7d.goods_id AND agg7d.dt = '2025-08-07'
         LEFT JOIN dws_goods_sales_agg_30d agg30d
                   ON gca.goods_id = agg30d.goods_id AND agg30d.dt = '2025-08-07'
WHERE gca.dt = '2025-08-07' AND gca.goods_status = 1;

SELECT * FROM ads_goods_sales_summary;

-- 3. 店铺销售汇总表
CREATE TABLE IF NOT EXISTS ads_shop_sales_summary (
                                                      shop_id           STRING      COMMENT '店铺ID',
                                                      shop_name         STRING      COMMENT '店铺名称',
                                                      shop_level        TINYINT     COMMENT '店铺等级',
                                                      shop_type         STRING      COMMENT '店铺类型',
                                                      main_category     STRING      COMMENT '主营类目',
                                                      last_7d_orders    BIGINT      COMMENT '近7日订单数',
                                                      last_7d_sales     BIGINT      COMMENT '近7日销量',
                                                      last_7d_revenue   DECIMAL(16,2) COMMENT '近7日销售额',
    last_7d_avg_order_value DECIMAL(10,2) COMMENT '近7日客单价',
    last_7d_buyers    BIGINT      COMMENT '近7日购买用户数',
    last_30d_orders   BIGINT      COMMENT '近30日订单数',
    last_30d_sales    BIGINT      COMMENT '近30日销量',
    last_30d_revenue  DECIMAL(16,2) COMMENT '近30日销售额',
    last_30d_avg_order_value DECIMAL(10,2) COMMENT '近30日客单价',
    last_30d_buyers   BIGINT      COMMENT '近30日购买用户数'
    )
    COMMENT '店铺销售汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/ads/ads_shop_sales_summary/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_shop_sales_summary PARTITION (dt='2025-08-07')
SELECT
    sbi.shop_id,
    sbi.shop_name,
    sbi.shop_level,
    sbi.shop_type,
    sbi.main_category,
    COALESCE(agg7d.total_orders, 0) AS last_7d_orders,
    COALESCE(agg7d.total_sales, 0) AS last_7d_sales,
    COALESCE(agg7d.total_revenue, 0) AS last_7d_revenue,
    COALESCE(agg7d.avg_order_value, 0) AS last_7d_avg_order_value,
    COALESCE(agg7d.distinct_buyers, 0) AS last_7d_buyers,
    COALESCE(agg30d.total_orders, 0) AS last_30d_orders,
    COALESCE(agg30d.total_sales, 0) AS last_30d_sales,
    COALESCE(agg30d.total_revenue, 0) AS last_30d_revenue,
    COALESCE(agg30d.avg_order_value, 0) AS last_30d_avg_order_value,
    COALESCE(agg30d.distinct_buyers, 0) AS last_30d_buyers
FROM ods_shop_base_info sbi
         LEFT JOIN dws_shop_sales_agg_7d agg7d
                   ON sbi.shop_id = agg7d.shop_id AND agg7d.dt = '2025-08-07'
         LEFT JOIN dws_shop_sales_agg_30d agg30d
                   ON sbi.shop_id = agg30d.shop_id AND agg30d.dt = '2025-08-07'
WHERE sbi.dt = '2025-08-07';

SELECT * FROM ads_shop_sales_summary;

-- 4. 用户购买行为汇总表
CREATE TABLE IF NOT EXISTS ads_user_purchase_summary (
                                                         user_id              STRING      COMMENT '用户ID',
                                                         user_name            STRING      COMMENT '用户名',
                                                         user_level           TINYINT     COMMENT '用户等级',
                                                         last_7d_orders       BIGINT      COMMENT '近7日订单数',
                                                         last_7d_spent        DECIMAL(16,2) COMMENT '近7日消费金额',
    last_7d_avg_order_value DECIMAL(10,2) COMMENT '近7日客单价',
    last_purchase_date   DATE        COMMENT '最后购买日期',
    favorite_category    STRING      COMMENT '最常购买类目',
    last_30d_orders      BIGINT      COMMENT '近30日订单数',
    last_30d_spent       DECIMAL(16,2) COMMENT '近30日消费金额',
    last_30d_avg_order_value DECIMAL(10,2) COMMENT '近30日客单价'
    )
    COMMENT '用户购买行为汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/ads/ads_user_purchase_summary/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_user_purchase_summary PARTITION (dt='2025-08-07')
SELECT
    ubi.user_id,
    ubi.user_name,
    ubi.user_level,
    COALESCE(p7d.total_orders, 0) AS last_7d_orders,
    COALESCE(p7d.total_spent, 0) AS last_7d_spent,
    COALESCE(p7d.avg_order_value, 0) AS last_7d_avg_order_value,
    COALESCE(p7d.last_purchase_date, '1970-01-01') AS last_purchase_date,
    COALESCE(p7d.favorite_category, '') AS favorite_category,
    COALESCE(p30d.total_orders, 0) AS last_30d_orders,
    COALESCE(p30d.total_spent, 0) AS last_30d_spent,
    COALESCE(p30d.avg_order_value, 0) AS last_30d_avg_order_value
FROM ods_user_base_info ubi
         LEFT JOIN dws_user_purchase_behavior_7d p7d
                   ON ubi.user_id = p7d.user_id AND p7d.dt = '2025-08-07'
         LEFT JOIN dws_user_purchase_behavior_30d p30d
                   ON ubi.user_id = p30d.user_id AND p30d.dt = '2025-08-07'
WHERE ubi.dt = '2025-08-07';

SELECT * FROM ads_user_purchase_summary;

-- 5. 商品行为分析汇总表
CREATE TABLE IF NOT EXISTS ads_goods_behavior_summary (
                                                          goods_id              STRING      COMMENT '商品ID',
                                                          goods_name            STRING      COMMENT '商品名称',
                                                          category_l1           STRING      COMMENT '一级类目',
                                                          category_l2           STRING      COMMENT '二级类目',
                                                          last_7d_view_count    BIGINT      COMMENT '近7日浏览次数',
                                                          last_7d_favor_count   BIGINT      COMMENT '近7日收藏次数',
                                                          last_7d_cart_count    BIGINT      COMMENT '近7日加购次数',
                                                          last_7d_purchase_count BIGINT     COMMENT '近7日购买次数',
                                                          last_7d_conversion_rate DOUBLE    COMMENT '近7日转化率',
                                                          last_7d_avg_stay_duration DOUBLE   COMMENT '近7日平均停留时长(秒)',
                                                          last_30d_view_count   BIGINT      COMMENT '近30日浏览次数',
                                                          last_30d_favor_count  BIGINT      COMMENT '近30日收藏次数',
                                                          last_30d_cart_count   BIGINT      COMMENT '近30日加购次数',
                                                          last_30d_purchase_count BIGINT    COMMENT '近30日购买次数',
                                                          last_30d_conversion_rate DOUBLE   COMMENT '近30日转化率',
                                                          last_30d_avg_stay_duration DOUBLE  COMMENT '近30日平均停留时长(秒)'
)
    COMMENT '商品行为分析汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall_05/ads/ads_goods_behavior_summary/'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_goods_behavior_summary PARTITION (dt='2025-08-07')
SELECT
    gca.goods_id,
    gca.goods_name,
    gca.category_l1,
    gca.category_l2,
    COALESCE(b7d.view_count, 0) AS last_7d_view_count,
    COALESCE(b7d.favor_count, 0) AS last_7d_favor_count,
    COALESCE(b7d.cart_count, 0) AS last_7d_cart_count,
    COALESCE(b7d.purchase_count, 0) AS last_7d_purchase_count,
    COALESCE(b7d.conversion_rate, 0) AS last_7d_conversion_rate,
    COALESCE(b7d.avg_stay_duration, 0) AS last_7d_avg_stay_duration,
    COALESCE(b30d.view_count, 0) AS last_30d_view_count,
    COALESCE(b30d.favor_count, 0) AS last_30d_favor_count,
    COALESCE(b30d.cart_count, 0) AS last_30d_cart_count,
    COALESCE(b30d.purchase_count, 0) AS last_30d_purchase_count,
    COALESCE(b30d.conversion_rate, 0) AS last_30d_conversion_rate,
    COALESCE(b30d.avg_stay_duration, 0) AS last_30d_avg_stay_duration
FROM ods_goods_core_attr gca
         LEFT JOIN dws_goods_behavior_analysis_7d b7d
                   ON gca.goods_id = b7d.goods_id AND b7d.dt = '2025-08-07'
         LEFT JOIN dws_goods_behavior_analysis_30d b30d
                   ON gca.goods_id = b30d.goods_id AND b30d.dt = '2025-08-07'
WHERE gca.dt = '2025-08-07' AND gca.goods_status = 1;

SELECT * FROM ads_goods_behavior_summary;