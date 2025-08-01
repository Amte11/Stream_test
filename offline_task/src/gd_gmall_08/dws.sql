SET hive.exec.mode.local.auto=true;
USE gmall_08;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

--1. 活动效果分析宽表
CREATE EXTERNAL TABLE ads_offer_activity_analysis (
    `activity_id` BIGINT COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `offer_type` STRING COMMENT '优惠类型',
    `daily_send` BIGINT COMMENT '当日发送量',
    `daily_redemption` BIGINT COMMENT '当日核销量',
    `daily_redemption_rate` DECIMAL(5,2) COMMENT '当日核销率',
    `weekly_avg_send` DECIMAL(10,2) COMMENT '周均发送量',
    `weekly_redemption` BIGINT COMMENT '周核销总量',
    `monthly_redemption_rate` DECIMAL(5,2) COMMENT '月核销率',
    `daily_savings` DECIMAL(15,2) COMMENT '当日节省金额',
    `weekly_savings` DECIMAL(15,2) COMMENT '周节省总额',
    `monthly_avg_daily_savings` DECIMAL(10,2) COMMENT '月日均节省'
) COMMENT '活动效果分析宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/ads/ads_offer_activity_analysis/';

INSERT OVERWRITE TABLE ads_offer_activity_analysis PARTITION(dt='2025-08-04')
SELECT
    a1d.activity_id,
    MAX(a1d.activity_name) AS activity_name,
    MAX(a1d.offer_type) AS offer_type,
    MAX(a1d.send_count) AS daily_send,
    MAX(a1d.redemption_count) AS daily_redemption,
    MAX(a1d.redemption_rate) AS daily_redemption_rate,
    MAX(a7d.avg_daily_send) AS weekly_avg_send,
    MAX(a7d.redemption_count) AS weekly_redemption,
    MAX(a30d.redemption_rate) AS monthly_redemption_rate,
    MAX(a1d.total_savings) AS daily_savings,
    MAX(a7d.total_savings) AS weekly_savings,
    MAX(a30d.avg_daily_savings) AS monthly_avg_daily_savings
FROM dws_offer_activity_1d a1d
         JOIN dws_offer_activity_7d a7d
              ON a1d.activity_id = a7d.activity_id AND a7d.dt='2025-08-04'
         JOIN dws_offer_activity_30d a30d
              ON a1d.activity_id = a30d.activity_id AND a30d.dt='2025-08-04'
WHERE a1d.dt='2025-08-04'
GROUP BY a1d.activity_id;
select * from ads_offer_activity_analysis;

--2.客服绩效分析宽表
CREATE EXTERNAL TABLE ads_cs_performance_analysis (
    `cs_id` BIGINT COMMENT '客服ID',
    `cs_name` STRING COMMENT '客服姓名',
    `department` STRING COMMENT '部门',
    `daily_send` BIGINT COMMENT '当日发送量',
    `daily_redemption` BIGINT COMMENT '当日核销量',
    `daily_avg_offer` DECIMAL(10,2) COMMENT '当日均优惠额',
    `weekly_avg_send` DECIMAL(10,2) COMMENT '周均发送量',
    `weekly_redemption_rate` DECIMAL(5,2) COMMENT '周核销率',
    `monthly_redemption_rate` DECIMAL(5,2) COMMENT '月核销率',
    `performance_score` DECIMAL(5,2) COMMENT '绩效得分'
) COMMENT '客服绩效分析宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/ads/ads_cs_performance_analysis/';

INSERT OVERWRITE TABLE ads_cs_performance_analysis PARTITION(dt='2025-08-04')
SELECT
    c1d.cs_id,
    MAX(c1d.cs_name) AS cs_name,
    MAX(c1d.department) AS department,
    MAX(c1d.send_count) AS daily_send,
    MAX(c1d.redemption_count) AS daily_redemption,
    MAX(c1d.avg_offer_amount) AS daily_avg_offer,
    MAX(c7d.avg_daily_send) AS weekly_avg_send,
    MAX(c7d.redemption_rate) AS weekly_redemption_rate,
    MAX(c30d.redemption_rate) AS monthly_redemption_rate,
    ROUND(
                (MAX(c1d.redemption_count) * 0.3) +
                (MAX(c7d.redemption_rate) * 0.4) +
                (MAX(c30d.redemption_rate) * 0.3),
                2
        ) AS performance_score
FROM dws_cs_service_1d c1d
         JOIN dws_cs_service_7d c7d
              ON c1d.cs_id = c7d.cs_id AND c7d.dt='2025-08-04'
         JOIN dws_cs_service_30d c30d
              ON c1d.cs_id = c30d.cs_id AND c30d.dt='2025-08-04'
WHERE c1d.dt='2025-08-04'
GROUP BY c1d.cs_id;

select * from ads_cs_performance_analysis;
--3. 客户价值分析宽表
CREATE EXTERNAL TABLE ads_customer_value_analysis (
    `customer_id` BIGINT COMMENT '客户ID',
    `daily_offer_count` BIGINT COMMENT '当日获券量',
    `daily_redemption_rate` DECIMAL(5,2) COMMENT '当日核销率',
    `weekly_offer_count` BIGINT COMMENT '周获券总量',
    `monthly_activity_level` STRING COMMENT '活跃等级',
    `daily_savings` DECIMAL(15,2) COMMENT '当日节省额',
    `monthly_avg_savings` DECIMAL(10,2) COMMENT '月日均节省',
    `value_index` DECIMAL(10,2) COMMENT '价值指数'
) COMMENT '客户价值分析宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/ads/ads_customer_value_analysis/';

INSERT OVERWRITE TABLE ads_customer_value_analysis PARTITION(dt='2025-08-04')
SELECT
    c1d.customer_id,
    MAX(c1d.offer_count) AS daily_offer_count,
    MAX(c1d.redemption_rate) AS daily_redemption_rate,
    MAX(c7d.offer_count) AS weekly_offer_count,
    MAX(c30d.activity_level) AS monthly_activity_level,
    MAX(c1d.total_savings) AS daily_savings,
    MAX(c30d.avg_monthly_savings) AS monthly_avg_savings,
    ROUND(
                (LOG(COALESCE(MAX(c30d.avg_monthly_savings), 0) + 1) * 0.6) +
                (CASE
                     WHEN MAX(c30d.activity_level) = '高价值' THEN 0.3
                     WHEN MAX(c30d.activity_level) = '中价值' THEN 0.2
                     ELSE 0.1
                    END) +
                (MAX(c1d.redemption_rate) * 0.1),
                2
        ) AS value_index
FROM dws_customer_offer_1d c1d
         JOIN dws_customer_offer_7d c7d
              ON c1d.customer_id = c7d.customer_id AND c7d.dt='2025-08-04'
         JOIN dws_customer_offer_30d c30d
              ON c1d.customer_id = c30d.customer_id AND c30d.dt='2025-08-04'
WHERE c1d.dt='2025-08-04'
GROUP BY c1d.customer_id;

select * from ads_customer_value_analysis;
--4.商品优惠效果分析宽表
CREATE EXTERNAL TABLE ads_product_offer_analysis (
    `sku_id` BIGINT COMMENT 'SKU ID',
    `daily_send_count` BIGINT COMMENT '当日发券量',
    `daily_redemption_rate` DECIMAL(5,2) COMMENT '当日核销率',
    `daily_avg_discount` DECIMAL(10,2) COMMENT '当日均折扣率',
    `weekly_redemption_rank` INT COMMENT '周核销排名',
    `monthly_savings` DECIMAL(15,2) COMMENT '月节省总额',
    `effectiveness_score` DECIMAL(5,2) COMMENT '优惠效能分'
) COMMENT '商品优惠效果分析宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/ads/ads_product_offer_analysis/';

INSERT OVERWRITE TABLE ads_product_offer_analysis PARTITION(dt='2025-08-04')
SELECT
    p1d.sku_id,
    MAX(p1d.send_count) AS daily_send_count,
    MAX(p1d.redemption_rate) AS daily_redemption_rate,
    MAX(p1d.avg_discount) AS daily_avg_discount,
    MIN(p30d.redemption_rank) AS best_redemption_rank,
    MAX(p30d.total_savings) AS monthly_savings,
    ROUND(
                (MAX(p1d.redemption_rate) * 0.5) +
                (1 - (MIN(p30d.redemption_rank) / MAX(max_rank.max_redemption_rank))) * 0.3 +  -- 修改点
                (MAX(p1d.avg_discount) * 0.2),
                2
        ) AS effectiveness_score
FROM dws_offer_products_1d p1d
         JOIN dws_offer_products_30d p30d
              ON p1d.sku_id = p30d.sku_id AND p30d.dt = '2025-08-04'
         CROSS JOIN (
    SELECT MAX(redemption_rank) AS max_redemption_rank
    FROM dws_offer_products_30d
    WHERE dt = '2025-08-04'
) max_rank
WHERE p1d.dt = '2025-08-04'
GROUP BY p1d.sku_id, max_rank.max_redemption_rank;
select * from ads_product_offer_analysis;
