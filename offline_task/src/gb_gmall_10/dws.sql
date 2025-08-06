CREATE DATABASE IF NOT EXISTS gmall_10;
USE gmall_10;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;

--一、页面访问聚合表（按时间维度）
--1. 1 天维度表（dws_page_visit_agg_1d）
-- 表结构创建
CREATE TABLE IF NOT EXISTS dws_page_visit_agg_1d (
                                                     page_id STRING COMMENT '页面ID',
                                                     page_type STRING COMMENT '页面类型',
                                                     hour_slot INT COMMENT '小时时段(0-23)',
                                                     visitor_count BIGINT COMMENT '访客数',
                                                     visit_pv BIGINT COMMENT '访问量',
                                                     click_pv BIGINT COMMENT '页面总点击量',
                                                     peak_visitor_count BIGINT COMMENT '高峰时段访客数',
                                                     peak_visit_pv BIGINT COMMENT '高峰时段访问量'
) COMMENT '页面访问1天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_visit_agg_1d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05）
INSERT OVERWRITE TABLE dws_page_visit_agg_1d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    page_type,
    hour_of_day AS hour_slot,
    SUM(visitor_count) AS visitor_count,
    SUM(visit_pv) AS visit_pv,
    SUM(click_pv) AS click_pv,
    SUM(CASE WHEN is_peak_hour THEN visitor_count ELSE 0 END) AS peak_visitor_count,
    SUM(CASE WHEN is_peak_hour THEN visit_pv ELSE 0 END) AS peak_visit_pv
FROM dwd_page_visit_detail
WHERE dt = '2025-08-05'
GROUP BY page_id, page_type, hour_of_day;

select  * FROM dws_page_visit_agg_1d;
--2. 7 天维度表（dws_page_visit_agg_7d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_visit_agg_7d (
                                                     page_id STRING COMMENT '页面ID',
                                                     page_type STRING COMMENT '页面类型',
                                                     hour_slot INT COMMENT '小时时段(0-23)',
                                                     visitor_count BIGINT COMMENT '访客数',
                                                     visit_pv BIGINT COMMENT '访问量',
                                                     click_pv BIGINT COMMENT '页面总点击量',
                                                     peak_visitor_count BIGINT COMMENT '高峰时段访客数',
                                                     peak_visit_pv BIGINT COMMENT '高峰时段访问量'
) COMMENT '页面访问7天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_visit_agg_7d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-30至2025-08-05）
INSERT OVERWRITE TABLE dws_page_visit_agg_7d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    page_type,
    hour_of_day AS hour_slot,
    SUM(visitor_count) AS visitor_count,
    SUM(visit_pv) AS visit_pv,
    SUM(click_pv) AS click_pv,
    SUM(CASE WHEN is_peak_hour THEN visitor_count ELSE 0 END) AS peak_visitor_count,
    SUM(CASE WHEN is_peak_hour THEN visit_pv ELSE 0 END) AS peak_visit_pv
FROM dwd_page_visit_detail
WHERE dt BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05'
GROUP BY page_id, page_type, hour_of_day;

select  * FROM dws_page_visit_agg_7d;
--3. 30 天维度表（dws_page_visit_agg_30d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_visit_agg_30d (
                                                      page_id STRING COMMENT '页面ID',
                                                      page_type STRING COMMENT '页面类型',
                                                      hour_slot INT COMMENT '小时时段(0-23)',
                                                      visitor_count BIGINT COMMENT '访客数',
                                                      visit_pv BIGINT COMMENT '访问量',
                                                      click_pv BIGINT COMMENT '页面总点击量',
                                                      peak_visitor_count BIGINT COMMENT '高峰时段访客数',
                                                      peak_visit_pv BIGINT COMMENT '高峰时段访问量'
) COMMENT '页面访问30天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_visit_agg_30d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-06至2025-08-05）
INSERT OVERWRITE TABLE dws_page_visit_agg_30d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    page_type,
    hour_of_day AS hour_slot,
    SUM(visitor_count) AS visitor_count,
    SUM(visit_pv) AS visit_pv,
    SUM(click_pv) AS click_pv,
    SUM(CASE WHEN is_peak_hour THEN visitor_count ELSE 0 END) AS peak_visitor_count,
    SUM(CASE WHEN is_peak_hour THEN visit_pv ELSE 0 END) AS peak_visit_pv
FROM dwd_page_visit_detail
WHERE dt BETWEEN DATE_SUB('2025-08-05', 29) AND '2025-08-05'
GROUP BY page_id, page_type, hour_of_day;

select  * FROM dws_page_visit_agg_30d;
--二、板块点击聚合表（按时间维度）
--1. 1 天维度表（dws_page_block_click_agg_1d）
-- 表结构创建
CREATE TABLE IF NOT EXISTS dws_page_block_click_agg_1d (
                                                           page_id STRING COMMENT '页面ID',
                                                           block_id STRING COMMENT '板块ID',
                                                           block_category STRING COMMENT '板块分类',
                                                           click_pv BIGINT COMMENT '点击量',
                                                           click_uv BIGINT COMMENT '点击人数',
                                                           guide_pay_amount DECIMAL(16,2) COMMENT '引导支付金额',
    avg_guide_pay_per_click DECIMAL(16,2) COMMENT '单次点击引导支付'
    ) COMMENT '板块点击1天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_block_click_agg_1d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05）
INSERT OVERWRITE TABLE dws_page_block_click_agg_1d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    block_id,
    block_category,
    SUM(click_pv) AS click_pv,
    SUM(click_uv) AS click_uv,
    SUM(guide_pay_amount) AS guide_pay_amount,
    ROUND(SUM(guide_pay_amount) / NULLIF(SUM(click_pv), 0), 2) AS avg_guide_pay_per_click
FROM dwd_page_block_click_fact
WHERE dt = '2025-08-05'
GROUP BY page_id, block_id, block_category;

select  * FROM dws_page_block_click_agg_1d;
--2. 7 天维度表（dws_page_block_click_agg_7d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_block_click_agg_7d (
                                                           page_id STRING COMMENT '页面ID',
                                                           block_id STRING COMMENT '板块ID',
                                                           block_category STRING COMMENT '板块分类',
                                                           click_pv BIGINT COMMENT '点击量',
                                                           click_uv BIGINT COMMENT '点击人数',
                                                           guide_pay_amount DECIMAL(16,2) COMMENT '引导支付金额',
    avg_guide_pay_per_click DECIMAL(16,2) COMMENT '单次点击引导支付'
    ) COMMENT '板块点击7天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_block_click_agg_7d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-30至2025-08-05）
INSERT OVERWRITE TABLE dws_page_block_click_agg_7d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    block_id,
    block_category,
    SUM(click_pv) AS click_pv,
    SUM(click_uv) AS click_uv,
    SUM(guide_pay_amount) AS guide_pay_amount,
    ROUND(SUM(guide_pay_amount) / NULLIF(SUM(click_pv), 0), 2) AS avg_guide_pay_per_click
FROM dwd_page_block_click_fact
WHERE dt BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05'
GROUP BY page_id, block_id, block_category;

select  * FROM dws_page_block_click_agg_7d;

--3. 30 天维度表（dws_page_block_click_agg_30d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_block_click_agg_30d (
                                                            page_id STRING COMMENT '页面ID',
                                                            block_id STRING COMMENT '板块ID',
                                                            block_category STRING COMMENT '板块分类',
                                                            click_pv BIGINT COMMENT '点击量',
                                                            click_uv BIGINT COMMENT '点击人数',
                                                            guide_pay_amount DECIMAL(16,2) COMMENT '引导支付金额',
    avg_guide_pay_per_click DECIMAL(16,2) COMMENT '单次点击引导支付'
    ) COMMENT '板块点击30天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_block_click_agg_30d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-06至2025-08-05）
INSERT OVERWRITE TABLE dws_page_block_click_agg_30d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    block_id,
    block_category,
    SUM(click_pv) AS click_pv,
    SUM(click_uv) AS click_uv,
    SUM(guide_pay_amount) AS guide_pay_amount,
    ROUND(SUM(guide_pay_amount) / NULLIF(SUM(click_pv), 0), 2) AS avg_guide_pay_per_click
FROM dwd_page_block_click_fact
WHERE dt BETWEEN DATE_SUB('2025-08-05', 29) AND '2025-08-05'
GROUP BY page_id, block_id, block_category;

select  * FROM dws_page_block_click_agg_30d;
--三、页面趋势聚合表（按时间维度）
--1. 1 天维度表（dws_page_trend_agg_1d）
-- 表结构创建
CREATE TABLE IF NOT EXISTS dws_page_trend_agg_1d (
                                                     page_id STRING COMMENT '页面ID',
                                                     visitor_count BIGINT COMMENT '访客数',
                                                     click_uv BIGINT COMMENT '点击人数',
                                                     week_visitor_count BIGINT COMMENT '本周累计访客',
                                                     weekend_visitor_count BIGINT COMMENT '周末访客数'
) COMMENT '页面趋势1天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_trend_agg_1d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05）
INSERT OVERWRITE TABLE dws_page_trend_agg_1d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    SUM(visitor_count) AS visitor_count,
    SUM(click_uv) AS click_uv,
    SUM(CASE WHEN is_current_week THEN visitor_count ELSE 0 END) AS week_visitor_count,
    SUM(CASE WHEN is_weekend THEN visitor_count ELSE 0 END) AS weekend_visitor_count
FROM dwd_page_data_trend_fact
WHERE dt = '2025-08-05'
GROUP BY page_id;

select  * FROM dws_page_trend_agg_1d;
--2. 7 天维度表（dws_page_trend_agg_7d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_trend_agg_7d (
                                                     page_id STRING COMMENT '页面ID',
                                                     visitor_count BIGINT COMMENT '访客数',
                                                     click_uv BIGINT COMMENT '点击人数',
                                                     week_visitor_count BIGINT COMMENT '本周累计访客',
                                                     weekend_visitor_count BIGINT COMMENT '周末访客数'
) COMMENT '页面趋势7天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_trend_agg_7d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-30至2025-08-05）
INSERT OVERWRITE TABLE dws_page_trend_agg_7d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    SUM(visitor_count) AS visitor_count,
    SUM(click_uv) AS click_uv,
    SUM(visitor_count) AS week_visitor_count,  -- 7天即整周数据
    SUM(CASE WHEN is_weekend THEN visitor_count ELSE 0 END) AS weekend_visitor_count
FROM dwd_page_data_trend_fact
WHERE stat_date BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05'
GROUP BY page_id;

select  * FROM dws_page_trend_agg_7d;
--3. 30 天维度表（dws_page_trend_agg_30d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_trend_agg_30d (
                                                      page_id STRING COMMENT '页面ID',
                                                      visitor_count BIGINT COMMENT '访客数',
                                                      click_uv BIGINT COMMENT '点击人数',
                                                      week_visitor_count BIGINT COMMENT '本周累计访客',
                                                      weekend_visitor_count BIGINT COMMENT '周末访客数'
) COMMENT '页面趋势30天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_trend_agg_30d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-06至2025-08-05）
INSERT OVERWRITE TABLE dws_page_trend_agg_30d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    SUM(visitor_count) AS visitor_count,
    SUM(click_uv) AS click_uv,
    SUM(CASE WHEN is_current_week THEN visitor_count ELSE 0 END) AS week_visitor_count,
    SUM(CASE WHEN is_weekend THEN visitor_count ELSE 0 END) AS weekend_visitor_count
FROM dwd_page_data_trend_fact
WHERE stat_date BETWEEN DATE_SUB('2025-08-05', 29) AND '2025-08-05'
GROUP BY page_id;

select  * FROM dws_page_trend_agg_30d;
--四、引导商品聚合表（按时间维度）
--1. 1 天维度表（dws_page_guide_product_agg_1d）
-- 表结构创建
CREATE TABLE IF NOT EXISTS dws_page_guide_product_agg_1d (
                                                             page_id STRING COMMENT '页面ID',
                                                             product_id STRING COMMENT '商品ID',
                                                             guide_count BIGINT COMMENT '引导次数',
                                                             guide_buyer_count BIGINT COMMENT '引导买家数',
                                                             high_value_guide_ratio DECIMAL(5,2) COMMENT '高价值引导占比'
    ) COMMENT '引导商品1天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_guide_product_agg_1d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05）
INSERT OVERWRITE TABLE dws_page_guide_product_agg_1d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    product_id,
    SUM(guide_count) AS guide_count,
    SUM(guide_buyer_count) AS guide_buyer_count,
    ROUND(
                    SUM(CASE WHEN is_high_value THEN guide_count ELSE 0 END) * 100.0
                / NULLIF(SUM(guide_count), 0),
                    2
        ) AS high_value_guide_ratio
FROM dwd_page_guide_product_fact
WHERE dt = '2025-08-05'
GROUP BY page_id, product_id;

select  * FROM dws_page_guide_product_agg_1d;
--2. 7 天维度表（dws_page_guide_product_agg_7d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_guide_product_agg_7d (
                                                             page_id STRING COMMENT '页面ID',
                                                             product_id STRING COMMENT '商品ID',
                                                             guide_count BIGINT COMMENT '引导次数',
                                                             guide_buyer_count BIGINT COMMENT '引导买家数',
                                                             high_value_guide_ratio DECIMAL(5,2) COMMENT '高价值引导占比'
    ) COMMENT '引导商品7天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_guide_product_agg_7d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-30至2025-08-05）
INSERT OVERWRITE TABLE dws_page_guide_product_agg_7d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    product_id,
    SUM(guide_count) AS guide_count,
    SUM(guide_buyer_count) AS guide_buyer_count,
    ROUND(
                    SUM(CASE WHEN is_high_value THEN guide_count ELSE 0 END) * 100.0
                / NULLIF(SUM(guide_count), 0),
                    2
        ) AS high_value_guide_ratio
FROM dwd_page_guide_product_fact
WHERE dt BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05'
GROUP BY page_id, product_id;

select  * FROM dws_page_guide_product_agg_7d;
--3. 30 天维度表（dws_page_guide_product_agg_30d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_guide_product_agg_30d (
                                                              page_id STRING COMMENT '页面ID',
                                                              product_id STRING COMMENT '商品ID',
                                                              guide_count BIGINT COMMENT '引导次数',
                                                              guide_buyer_count BIGINT COMMENT '引导买家数',
                                                              high_value_guide_ratio DECIMAL(5,2) COMMENT '高价值引导占比'
    ) COMMENT '引导商品30天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_guide_product_agg_30d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-06至2025-08-05）
INSERT OVERWRITE TABLE dws_page_guide_product_agg_30d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    product_id,
    SUM(guide_count) AS guide_count,
    SUM(guide_buyer_count) AS guide_buyer_count,
    ROUND(
                    SUM(CASE WHEN is_high_value THEN guide_count ELSE 0 END) * 100.0
                / NULLIF(SUM(guide_count), 0),
                    2
        ) AS high_value_guide_ratio
FROM dwd_page_guide_product_fact
WHERE dt BETWEEN DATE_SUB('2025-08-05', 29) AND '2025-08-05'
GROUP BY page_id, product_id;

select  * FROM dws_page_guide_product_agg_30d;
--五、页面模块聚合表（按时间维度）
--1. 1 天维度表（dws_page_module_agg_1d）
-- 表结构创建
CREATE TABLE IF NOT EXISTS dws_page_module_agg_1d (
                                                      page_id STRING COMMENT '页面ID',
                                                      module_id STRING COMMENT '模块ID',
                                                      module_type STRING COMMENT '模块类型',
                                                      expose_pv BIGINT COMMENT '曝光量',
                                                      interact_count BIGINT COMMENT '互动量',
                                                      effective_interaction_rate DECIMAL(5,2) COMMENT '有效互动率'
    ) COMMENT '页面模块1天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_module_agg_1d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05）
INSERT OVERWRITE TABLE dws_page_module_agg_1d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    module_id,
    module_type,
    SUM(expose_pv) AS expose_pv,
    SUM(interact_count) AS interact_count,
    ROUND(
                    SUM(interact_count) * 100.0 / NULLIF(SUM(expose_pv), 0),
                    2
        ) AS effective_interaction_rate
FROM dwd_page_module_detail_fact
WHERE dt = '2025-08-05'
GROUP BY page_id, module_id, module_type;

select  * FROM dws_page_module_agg_1d;
--2. 7 天维度表（dws_page_module_agg_7d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_module_agg_7d (
                                                      page_id STRING COMMENT '页面ID',
                                                      module_id STRING COMMENT '模块ID',
                                                      module_type STRING COMMENT '模块类型',
                                                      expose_pv BIGINT COMMENT '曝光量',
                                                      interact_count BIGINT COMMENT '互动量',
                                                      effective_interaction_rate DECIMAL(5,2) COMMENT '有效互动率'
    ) COMMENT '页面模块7天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_module_agg_7d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-30至2025-08-05）
INSERT OVERWRITE TABLE dws_page_module_agg_7d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    module_id,
    module_type,
    SUM(expose_pv) AS expose_pv,
    SUM(interact_count) AS interact_count,
    ROUND(
                    SUM(interact_count) * 100.0 / NULLIF(SUM(expose_pv), 0),
                    2
        ) AS effective_interaction_rate
FROM dwd_page_module_detail_fact
WHERE dt BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05'
GROUP BY page_id, module_id, module_type;

select  * FROM dws_page_module_agg_7d;
--3. 30 天维度表（dws_page_module_agg_30d）
-- 表结构创建（与1d一致）
CREATE TABLE IF NOT EXISTS dws_page_module_agg_30d (
                                                       page_id STRING COMMENT '页面ID',
                                                       module_id STRING COMMENT '模块ID',
                                                       module_type STRING COMMENT '模块类型',
                                                       expose_pv BIGINT COMMENT '曝光量',
                                                       interact_count BIGINT COMMENT '互动量',
                                                       effective_interaction_rate DECIMAL(5,2) COMMENT '有效互动率'
    ) COMMENT '页面模块30天聚合表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dws/dws_page_module_agg_30d'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 数据加载（dt=2025-08-05，时间范围：2025-07-06至2025-08-05）
INSERT OVERWRITE TABLE dws_page_module_agg_30d PARTITION(dt='2025-08-05')
SELECT
    page_id,
    module_id,
    module_type,
    SUM(expose_pv) AS expose_pv,
    SUM(interact_count) AS interact_count,
    ROUND(
                    SUM(interact_count) * 100.0 / NULLIF(SUM(expose_pv), 0),
                    2
        ) AS effective_interaction_rate
FROM dwd_page_module_detail_fact
WHERE dt BETWEEN DATE_SUB('2025-08-05', 29) AND '2025-08-05'
GROUP BY page_id, module_id, module_type;

select  * FROM dws_page_module_agg_30d;