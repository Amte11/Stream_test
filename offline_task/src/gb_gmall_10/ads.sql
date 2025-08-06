CREATE DATABASE IF NOT EXISTS gmall_10;
USE gmall_10;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

-- 1. 页面概览报表
DROP TABLE IF EXISTS ads_page_overview_daily;
CREATE TABLE ads_page_overview_daily (
                                         page_id STRING COMMENT '页面ID',
                                         page_type STRING COMMENT '页面类型',
                                         total_visitor_count_1d BIGINT COMMENT '当日访客数',
                                         total_visit_pv_1d BIGINT COMMENT '当日访问量',
                                         avg_click_per_visit_1d DECIMAL(10,2) COMMENT '人均点击量',
                                         peak_visitor_ratio_1d DECIMAL(5,2) COMMENT '高峰访客占比(%)',
                                         guide_pay_conversion_rate_7d DECIMAL(5,2) COMMENT '7日引导支付转化率(%)',
                                         interaction_rate_30d DECIMAL(5,2) COMMENT '30日平均互动率(%)'
) COMMENT '页面概览报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall_10/ads/ads_page_overview_daily'
TBLPROPERTIES ("orc.compress"="snappy");

INSERT OVERWRITE TABLE ads_page_overview_daily PARTITION(dt)
SELECT
    pvo.page_id,
    pvo.page_type,
    COALESCE(SUM(pvo.visitor_count), 0),
    COALESCE(SUM(pvo.visit_pv), 0),
    ROUND(COALESCE(SUM(pvo.click_pv), 0) * 1.0 / NULLIF(SUM(pvo.visitor_count), 1), 2),
    ROUND(COALESCE(SUM(pvo.peak_visitor_count), 0) * 100.0 / NULLIF(SUM(pvo.visitor_count), 1), 2),
    ROUND(COALESCE(SUM(bc7.guide_pay_amount), 0) * 100.0 / NULLIF(SUM(bc7.click_pv), 1), 2),
    ROUND(COALESCE(SUM(mm30.interact_count), 0) * 100.0 / NULLIF(SUM(mm30.expose_pv), 1), 2),
    '2025-08-05' AS dt  -- 分区值作为最后一列
FROM dws_page_visit_agg_1d pvo
         LEFT JOIN dws_page_block_click_agg_7d bc7
                   ON pvo.page_id = bc7.page_id AND bc7.dt='2025-08-05'
         LEFT JOIN dws_page_module_agg_30d mm30
                   ON pvo.page_id = mm30.page_id AND mm30.dt='2025-08-05'
WHERE pvo.dt='2025-08-05'
GROUP BY pvo.page_id, pvo.page_type;

select  * from ads_page_overview_daily;

-- 2. 板块点击分布报表（修复分区冲突）
DROP TABLE IF EXISTS ads_block_performance_daily;
CREATE TABLE ads_block_performance_daily (
                                             block_id STRING COMMENT '板块ID',
                                             block_category STRING COMMENT '板块分类',
                                             click_pv_1d BIGINT COMMENT '当日点击量',
                                             click_uv_1d BIGINT COMMENT '当日点击人数',
                                             guide_pay_amount_1d DECIMAL(16,2) COMMENT '当日引导支付金额',
                                             avg_guide_pay_1d DECIMAL(16,2) COMMENT '单次点击引导支付(元)',
                                             click_share_30d DECIMAL(5,2) COMMENT '30日点击量占比(%)',
                                             high_value_guide_ratio_7d DECIMAL(5,2) COMMENT '7日高价值引导占比(%)'
) COMMENT '板块点击分布报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall_10/ads/ads_block_performance_daily'
TBLPROPERTIES ("orc.compress"="snappy");

INSERT OVERWRITE TABLE ads_block_performance_daily PARTITION(dt)
SELECT
    bc1.block_id,
    bc1.block_category,
    COALESCE(SUM(bc1.click_pv), 0),
    COALESCE(SUM(bc1.click_uv), 0),
    COALESCE(SUM(bc1.guide_pay_amount), 0),
    COALESCE(SUM(bc1.avg_guide_pay_per_click), 0),
    ROUND(COALESCE(SUM(bc1.click_pv), 0) * 100.0 / NULLIF(SUM(bc30_total.click_pv_total), 0), 2),
    ROUND(COALESCE(SUM(gp7.high_value_guide_ratio), 0) / NULLIF(COUNT(DISTINCT gp7.product_id), 0), 2),
    '2025-08-05' AS dt  -- 分区值作为最后一列
FROM dws_page_block_click_agg_1d bc1
         LEFT JOIN (
    SELECT block_id, SUM(click_pv) AS click_pv_total
    FROM dws_page_block_click_agg_30d
    WHERE dt = '2025-08-05'
    GROUP BY block_id
) bc30_total ON bc1.block_id = bc30_total.block_id
         LEFT JOIN dws_page_guide_product_agg_7d gp7
                   ON bc1.page_id = gp7.page_id AND gp7.dt='2025-08-05'
WHERE bc1.dt='2025-08-05'
GROUP BY bc1.block_id, bc1.block_category;
select * from ads_block_performance_daily;

-- 3. 页面趋势分析报表（修复分区冲突）
DROP TABLE IF EXISTS ads_page_trend_daily;
CREATE TABLE ads_page_trend_daily (
                                      page_id STRING COMMENT '页面ID',
                                      visitor_count_1d BIGINT COMMENT '当日访客数',
                                      week_visitor_count BIGINT COMMENT '本周累计访客',
                                      weekend_visitor_ratio DECIMAL(5,2) COMMENT '周末访客占比(%)',
                                      visitor_growth_7d DECIMAL(10,2) COMMENT '7日访客增长率(%)',
                                      click_uv_30d BIGINT COMMENT '30日点击总人数',
                                      click_growth_30d DECIMAL(10,2) COMMENT '30日点击人数增长率(%)'
) COMMENT '页面趋势分析报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall_10/ads/ads_page_trend_daily'
TBLPROPERTIES ("orc.compress"="snappy");

INSERT OVERWRITE TABLE ads_page_trend_daily PARTITION(dt)
SELECT
    tt1.page_id,
    COALESCE(SUM(tt1.visitor_count), 0) AS visitor_count_1d,
    COALESCE(SUM(tt7.week_visitor_count), 0) AS week_visitor_count,
    ROUND(COALESCE(SUM(tt7.weekend_visitor_count), 0) * 100.0 / NULLIF(SUM(tt7.visitor_count), 0), 2) AS weekend_visitor_ratio,
    ROUND((COALESCE(SUM(tt7.visitor_count), 0) - COALESCE(SUM(prev7.visitor_count), 0)) * 100.0
              / NULLIF(COALESCE(SUM(prev7.visitor_count), 0), 0), 2) AS visitor_growth_7d,
    COALESCE(SUM(tt30.click_uv), 0) AS click_uv_30d,
    ROUND((COALESCE(SUM(tt30.click_uv), 0) - COALESCE(t30_prev.click_uv, 0))
              * 100.0 / NULLIF(COALESCE(t30_prev.click_uv, 0), 0), 2) AS click_growth_30d,
    '2025-08-05' AS dt
FROM dws_page_trend_agg_1d tt1
         JOIN dws_page_trend_agg_7d tt7
              ON tt1.page_id = tt7.page_id AND tt7.dt='2025-08-05'
         JOIN dws_page_trend_agg_30d tt30
              ON tt1.page_id = tt30.page_id AND tt30.dt='2025-08-05'
         LEFT JOIN dws_page_trend_agg_7d prev7
                   ON tt1.page_id = prev7.page_id AND prev7.dt=DATE_SUB('2025-08-05', 7)
         LEFT JOIN (
    SELECT
        page_id,
        SUM(click_uv) AS click_uv
    FROM dws_page_trend_agg_30d
    WHERE dt = DATE_SUB('2025-08-05', 1)
    GROUP BY page_id
) t30_prev ON tt30.page_id = t30_prev.page_id
WHERE tt1.dt='2025-08-05'
-- 关键修改：将所有关联表的page_id和非聚合字段纳入GROUP BY
GROUP BY
    tt1.page_id,
    tt7.page_id,
    tt30.page_id,
    prev7.page_id,
    t30_prev.page_id,
    t30_prev.click_uv;
select * from ads_page_trend_daily;

-- 4. 商品引导详情报表（修复分区冲突）
DROP TABLE IF EXISTS ads_product_guide_conversion_daily;
CREATE TABLE ads_product_guide_conversion_daily (
                                                    product_id STRING COMMENT '商品ID',
                                                    product_name STRING COMMENT '商品名称',
                                                    guide_count_1d BIGINT COMMENT '当日引导次数',
                                                    guide_buyer_count_1d BIGINT COMMENT '当日引导买家数',
                                                    high_value_guide_ratio_1d DECIMAL(5,2) COMMENT '高价值引导占比(%)',
                                                    avg_daily_guide_7d DECIMAL(10,2) COMMENT '7日日均引导量',
                                                    guide_conversion_rate_30d DECIMAL(5,2) COMMENT '30日引导转化率(%)'
) COMMENT '商品引导详情报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall_10/ads/ads_product_guide_conversion_daily'
TBLPROPERTIES ("orc.compress"="snappy");

INSERT OVERWRITE TABLE ads_product_guide_conversion_daily PARTITION(dt)
SELECT
    gp1.product_id,
    dim_product.product_name,
    COALESCE(SUM(gp1.guide_count), 0),
    COALESCE(SUM(gp1.guide_buyer_count), 0),
    COALESCE(AVG(gp1.high_value_guide_ratio), 0),
    COALESCE(SUM(gp7.guide_count) / 7.0, 0),
    ROUND(COALESCE(SUM(gp30.guide_buyer_count), 0) * 100.0 / NULLIF(SUM(gp30.guide_count), 1), 2),
    '2025-08-05' AS dt  -- 分区值作为最后一列
FROM dws_page_guide_product_agg_1d gp1
         JOIN dws_page_guide_product_agg_7d gp7
              ON gp1.product_id = gp7.product_id AND gp7.dt='2025-08-05'
         JOIN dws_page_guide_product_agg_30d gp30
              ON gp1.product_id = gp30.product_id AND gp30.dt='2025-08-05'
         LEFT JOIN dim_product_info_zip dim_product
                   ON gp1.product_id = dim_product.product_id
WHERE gp1.dt='2025-08-05'
GROUP BY gp1.product_id, dim_product.product_name;

select * from ads_product_guide_conversion_daily;

-- 5. 页面模块明细报表（修复分区冲突）
DROP TABLE IF EXISTS ads_module_performance_daily;
CREATE TABLE ads_module_performance_daily (
                                              module_id STRING COMMENT '模块ID',
                                              module_type STRING COMMENT '模块类型',
                                              expose_pv_1d BIGINT COMMENT '当日曝光量',
                                              interact_count_1d BIGINT COMMENT '当日互动量',
                                              effective_interaction_rate_1d DECIMAL(5,2) COMMENT '有效互动率(%)',
                                              avg_daily_interact_7d BIGINT COMMENT '7日日均互动量',
                                              exposure_growth_30d DECIMAL(10,2) COMMENT '30日曝光增长率(%)'
) COMMENT '页面模块明细报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall_10/ads/ads_module_performance_daily'
TBLPROPERTIES ("orc.compress"="snappy");

INSERT OVERWRITE TABLE ads_module_performance_daily PARTITION(dt='2025-08-05')
SELECT
    mm1.module_id,
    mm1.module_type,
    COALESCE(SUM(mm1.expose_pv), 0) AS expose_pv_1d,
    COALESCE(SUM(mm1.interact_count), 0) AS interact_count_1d,
    ROUND(COALESCE(AVG(mm1.effective_interaction_rate), 0), 2) AS effective_interaction_rate_1d,
    COALESCE(ROUND(SUM(mm7.interact_count) / 7.0, 0), 0) AS avg_daily_interact_7d,
    ROUND(
                    (COALESCE(MAX(current_30.expose_pv_sum), 0) - COALESCE(MIN(prev_30.expose_pv_sum), 0))
                    * 100.0 / NULLIF(COALESCE(MAX(prev_30.expose_pv_sum), 1), 0),
                    2
        ) AS exposure_growth_30d
FROM dws_page_module_agg_1d mm1
         JOIN dws_page_module_agg_7d mm7
              ON mm1.module_id = mm7.module_id AND mm7.dt = '2025-08-05'
         JOIN (
    SELECT
        module_id,
        SUM(expose_pv) AS expose_pv_sum
    FROM dws_page_module_agg_30d
    WHERE dt = '2025-08-05'
    GROUP BY module_id
) current_30 ON mm1.module_id = current_30.module_id
         LEFT JOIN (
    SELECT
        module_id,
        SUM(expose_pv) AS expose_pv_sum
    FROM dws_page_module_agg_30d
    WHERE dt = DATE_SUB('2025-08-05', 30)
    GROUP BY module_id
) prev_30 ON mm1.module_id = prev_30.module_id
WHERE mm1.dt = '2025-08-05'
GROUP BY
    mm1.module_id,
    mm1.module_type;
select * from ads_module_performance_daily;
