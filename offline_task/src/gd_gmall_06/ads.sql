set hive.exec.mode.local.auto=True;
USE gmall_05;

SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

-- 1. 连带用户价值分层
DROP TABLE IF EXISTS ads_user_value_tier;
CREATE TABLE ads_user_value_tier (
                                     user_id         STRING      COMMENT '用户ID',
                                     user_name       STRING      COMMENT '用户名',
                                     user_level      TINYINT     COMMENT '用户等级',
                                     active_tier     STRING      COMMENT '连带活跃层级', -- 高/中/低连带活跃
                                     purchase_tier   STRING      COMMENT '连带购买力层级', -- 高/中/低连带价值
                                     login_days_7d   INT         COMMENT '近7日登录天数',
                                     view_count_7d   BIGINT      COMMENT '近7日连带浏览数',
                                     purchase_count_7d BIGINT    COMMENT '近7日连带购买次数',
                                     total_spent_30d DECIMAL(16,2) COMMENT '近30日连带消费金额',
                                     last_active_date DATE       COMMENT '最后连带活跃日期'
) COMMENT '连带用户价值分层报告'
PARTITIONED BY (dt STRING)
STORED AS ORC
location '/warehouse/gmall_05/ads/ads_user_value_tier'
tblproperties ('orc.compress' = 'SNAPPY');

-- 1. 连带用户价值分层
INSERT OVERWRITE TABLE ads_user_value_tier PARTITION (dt='2025-08-07')
SELECT
    ub.user_id,
    ub.user_name,
    ub.user_level,
    CASE
        WHEN agg7d.view_count > 100 THEN '高活跃'
        WHEN agg7d.view_count > 30 THEN '中活跃'
        ELSE '低活跃'
        END AS active_tier,
    CASE
        WHEN pur30d.total_spent > 5000 THEN '高价值'
        WHEN pur30d.total_spent > 1000 THEN '中价值'
        ELSE '低价值'
        END AS purchase_tier,
    login_agg.login_days_7d,
    COALESCE(agg7d.view_count, 0),
    COALESCE(pur7d.total_orders, 0),
    COALESCE(pur30d.total_spent, 0),
    MAX(DATE(ubl.behavior_time)) AS last_active_date
FROM dws_user_behavior_agg_7d agg7d
         JOIN dws_user_purchase_behavior_30d pur30d
              ON agg7d.user_id = pur30d.user_id
         LEFT JOIN dws_user_purchase_behavior_7d pur7d
                   ON agg7d.user_id = pur7d.user_id
         JOIN ods_user_base_info ub
              ON agg7d.user_id = ub.user_id
         LEFT JOIN (
    SELECT
        user_id,
        COUNT(DISTINCT DATE(behavior_time)) AS login_days_7d
    FROM ods_user_behavior_log
    WHERE dt BETWEEN '2025-08-01' AND '2025-08-07'
    GROUP BY user_id
) login_agg
                   ON ub.user_id = login_agg.user_id
         LEFT JOIN ods_user_behavior_log ubl
                   ON agg7d.user_id = ubl.user_id AND ubl.dt='2025-08-07'
WHERE agg7d.dt='2025-08-07'
  AND pur30d.dt='2025-08-07'
  AND pur7d.dt='2025-08-07'
GROUP BY
    ub.user_id,
    ub.user_name,
    ub.user_level,
    agg7d.view_count,
    pur30d.total_spent,
    pur7d.total_orders,
    login_agg.login_days_7d;

select  * FROM ads_user_value_tier;

-- 2. 商品连带热度榜
CREATE TABLE ads_goods_hot_rank (
                                    goods_id        STRING      COMMENT '商品ID',
                                    goods_name      STRING      COMMENT '商品名称',
                                    category_l1     STRING      COMMENT '一级类目',
                                    hot_score       DECIMAL(10,2) COMMENT '连带热度分数',
                                    view_rank       INT         COMMENT '连带浏览排名',
                                    purchase_rank   INT         COMMENT '连带购买排名',
                                    conversion_rate DOUBLE      COMMENT '连带转化率',
                                    total_sales_7d  BIGINT      COMMENT '近7日连带销量',
                                    revenue_7d      DECIMAL(16,2) COMMENT '近7日连带销售额'
) COMMENT '商品连带热度榜'
PARTITIONED BY (dt STRING)
STORED AS ORC
location '/warehouse/gmall_05/ads/ads_goods_hot_rank'
tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 商品连带热度榜
INSERT OVERWRITE TABLE ads_goods_hot_rank PARTITION (dt='2025-08-07')
SELECT
    ga.goods_id,
    ga.goods_name,
    ga.category_l1,
    ROUND(
                (0.3 * (1 - PERCENT_RANK() OVER (ORDER BY ba.view_count DESC)) +
                 0.5 * (1 - PERCENT_RANK() OVER (ORDER BY sa.total_sales DESC)) +
                 0.2 * COALESCE(st.growth_rate, 0)) * 100, 2) AS hot_score,
    RANK() OVER (ORDER BY ba.view_count DESC) AS view_rank,
        RANK() OVER (ORDER BY sa.total_sales DESC) AS purchase_rank,
        ba.conversion_rate,
    sa.total_sales,
    sa.total_revenue
FROM dws_goods_behavior_analysis_1d ba
         JOIN dws_goods_sales_agg_7d sa
              ON ba.goods_id = sa.goods_id AND ba.dt = sa.dt
         JOIN ods_goods_core_attr ga
              ON ba.goods_id = ga.goods_id
         LEFT JOIN (
    SELECT
        goods_id,
        (sum_7d - sum_prev) / NULLIF(sum_prev, 0) AS growth_rate
    FROM (
             SELECT
                 goods_id,
                 SUM(CASE WHEN dt BETWEEN '2025-08-01' AND '2025-08-07' THEN total_sales END) AS sum_7d,
                 SUM(CASE WHEN dt BETWEEN '2025-07-24' AND '2025-07-30' THEN total_sales END) AS sum_prev
             FROM dws_goods_sales_agg_1d
             WHERE dt BETWEEN '2025-07-24' AND '2025-08-07'
             GROUP BY goods_id
         ) sales_data
) st
                   ON ba.goods_id = st.goods_id
WHERE ba.dt = '2025-08-07'
  AND sa.dt = '2025-08-07'
  AND ga.goods_status = 1;

SELECT * FROM ads_goods_hot_rank;

-- 3. 店铺连带竞争力
drop table if exists ads_shop_competitiveness;
CREATE TABLE ads_shop_competitiveness (
                                          shop_id         STRING      COMMENT '店铺ID',
                                          shop_name       STRING      COMMENT '店铺名称',
                                          comp_index      DECIMAL(10,2) COMMENT '连带竞争力指数',
                                          sales_growth_rate DOUBLE    COMMENT '连带销售增长率',
                                          buyer_repurchase_rate DOUBLE COMMENT '买家连带复购率',
                                          avg_order_value DECIMAL(10,2) COMMENT '连带客单价',
                                          buyer_count_7d  BIGINT      COMMENT '近7日连带买家数',
                                          top_3_goods     STRING      COMMENT '连带热销TOP3商品'
) COMMENT '店铺连带竞争力评估'
PARTITIONED BY (dt STRING)
STORED AS ORC
location '/warehouse/gmall_05/ads/ads_shop_competitiveness'
tblproperties ('orc.compress' = 'SNAPPY');


-- 3. 店铺连带竞争力
INSERT OVERWRITE TABLE ads_shop_competitiveness PARTITION (dt='2025-08-07')
SELECT
    sa.shop_id,
    sa.shop_name,
    ROUND(
            (0.4 * COALESCE(
                    CASE
                        WHEN sales_data.prev_sales IS NULL OR sales_data.prev_sales = 0 THEN 1
                        ELSE COALESCE(sales_data.current_sales, 0) / sales_data.prev_sales
                        END, 1) +
             0.6 * COALESCE(repurchase_rate, 0)), 2) AS comp_index,
    COALESCE(
            CASE
                WHEN sales_data.prev_sales IS NULL OR sales_data.prev_sales = 0 THEN 0
                ELSE (COALESCE(sales_data.current_sales, 0) - sales_data.prev_sales) / sales_data.prev_sales
                END, 0) AS sales_growth_rate,
    COALESCE(repurchase_rate, 0),
    COALESCE(sa.avg_order_value, 0),
    COALESCE(sa.distinct_buyers, 0),
    CONCAT_WS(',', COLLECT_LIST(goods_name)) AS top_3_goods
FROM (
         SELECT
             shop_id,
             SUM(total_sales) AS current_sales,
             LAG(SUM(total_sales), 7) OVER(PARTITION BY shop_id ORDER BY dt) AS prev_sales
         FROM dws_shop_sales_agg_1d
         WHERE dt BETWEEN '2025-07-31' AND '2025-08-07'
         GROUP BY shop_id, dt
     ) sales_data
         JOIN (
    SELECT *
    FROM dws_shop_sales_agg_7d
    WHERE dt = '2025-08-07'
) sa ON sales_data.shop_id = sa.shop_id
         LEFT JOIN (
    -- 店铺复购率计算
    SELECT
        shop_id,
        COALESCE(COUNT(CASE WHEN order_count > 1 THEN user_id END) * 1.0 / NULLIF(COUNT(user_id), 0), 0) AS repurchase_rate
    FROM (
             SELECT
                 shop_id,
                 user_id,
                 COUNT(order_id) AS order_count
             FROM ods_order_trade_fact
             WHERE dt BETWEEN '2025-08-01' AND '2025-08-07'
             GROUP BY shop_id, user_id
         ) t
    GROUP BY shop_id
) rep ON sa.shop_id = rep.shop_id
         LEFT JOIN (
    -- 取店铺热销TOP3商品
    SELECT
        shop_id,
        goods_name
    FROM (
             SELECT
                 otf.shop_id,
                 ga.goods_name,
                 ROW_NUMBER() OVER(PARTITION BY otf.shop_id ORDER BY SUM(ogd.goods_quantity) DESC) AS rn
             FROM ods_order_goods_detail ogd
                      JOIN ods_order_trade_fact otf
                           ON ogd.order_id = otf.order_id
                      JOIN ods_goods_core_attr ga
                           ON ogd.goods_id = ga.goods_id
             WHERE ogd.dt = '2025-08-07'
               AND otf.pay_status = 1
             GROUP BY otf.shop_id, ga.goods_name
         ) t
    WHERE rn <= 3
) goods ON sa.shop_id = goods.shop_id
GROUP BY
    sa.shop_id,
    sa.shop_name,
    sales_data.current_sales,
    sales_data.prev_sales,
    rep.repurchase_rate,
    sa.avg_order_value,
    sa.distinct_buyers;
SELECT * FROM ads_shop_competitiveness;


-- 4. 品类连带转化漏斗
CREATE TABLE ads_category_conversion (
                                         category_l1     STRING      COMMENT '一级类目',
                                         category_l2     STRING      COMMENT '二级类目',
                                         view_count      BIGINT      COMMENT '连带浏览量',
                                         cart_add_rate   DOUBLE      COMMENT '连带加购率',
                                         favor_rate      DOUBLE      COMMENT '连带收藏率',
                                         purchase_rate   DOUBLE      COMMENT '连带购买转化率',
                                         lost_rate       DOUBLE      COMMENT '连带流失率'
) COMMENT '品类连带转化漏斗'
PARTITIONED BY (dt STRING)
STORED AS ORC
location '/warehouse/gmall_05/ads/ads_category_conversion'
tblproperties ('orc.compress' = 'SNAPPY');


-- 4. 品类连带转化漏斗
INSERT OVERWRITE TABLE ads_category_conversion PARTITION (dt='2025-08-07')
SELECT
    ca.category_l1,
    ca.category_l2,
    SUM(ba.view_count) AS view_count,
    SUM(ba.cart_count) / NULLIF(SUM(ba.view_count), 0) AS cart_add_rate,
    SUM(ba.favor_count) / NULLIF(SUM(ba.view_count), 0) AS favor_rate,
    SUM(ba.purchase_count) / NULLIF(SUM(ba.view_count), 0) AS purchase_rate,
    1 - (SUM(ba.purchase_count) / NULLIF(SUM(ba.view_count), 0)) AS lost_rate
FROM dws_goods_behavior_analysis_1d ba
         LEFT JOIN ods_goods_core_attr ca ON ba.goods_id = ca.goods_id
WHERE ba.dt = '2025-08-07'
  AND ca.category_l1 IS NOT NULL
  AND ca.category_l2 IS NOT NULL
GROUP BY ca.category_l1, ca.category_l2;


select * FROM ads_category_conversion;

-- 5. 商品连带销售预测
CREATE TABLE ads_goods_sales_forecast (
                                          goods_id        STRING      COMMENT '商品ID',
                                          goods_name      STRING      COMMENT '商品名称',
                                          category_l1     STRING      COMMENT '一级类目',
                                          category_l2     STRING      COMMENT '二级类目',
                                          category_l3     STRING      COMMENT '三级类目',
                                          sales_forecast  BIGINT      COMMENT '连带销售预测值'
) COMMENT '商品连带销售预测'
PARTITIONED BY (dt STRING)
STORED AS ORC
location '/warehouse/gmall_05/ads/ads_goods_sales_forecast'
tblproperties ('orc.compress' = 'SNAPPY');

-- 5. 商品连带销售预测
INSERT OVERWRITE TABLE ads_goods_sales_forecast PARTITION (dt='2025-08-07')
SELECT
    goods_id,
    goods_name,
    category_l1,
    category_l2,
    category_l3,
    ROUND(SUM(total_sales) * 1.2) AS sales_forecast
FROM dws_goods_sales_agg_7d
WHERE dt='2025-08-07'
GROUP BY goods_id, goods_name, category_l1, category_l2,
         category_l3;

select * FROM ads_goods_sales_forecast;


