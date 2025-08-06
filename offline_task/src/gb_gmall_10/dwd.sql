-- 创建DWD层数据库
CREATE DATABASE IF NOT EXISTS gmall_10;
USE gmall_10;

-- 1. 页面访问明细事实表 (dwd_page_visit_detail)
CREATE TABLE IF NOT EXISTS dwd_page_visit_detail (
                                                     visit_id STRING COMMENT '访问唯一ID',
                                                     page_id STRING COMMENT '页面ID',
                                                     page_type STRING COMMENT '页面类型',
                                                     visit_time TIMESTAMP COMMENT '访问时间',
                                                     visitor_count BIGINT COMMENT '访客数',
                                                     visit_pv BIGINT COMMENT '访问量',
                                                     click_pv BIGINT COMMENT '页面总点击量',
                                                     hour_of_day INT COMMENT '小时时段(0-23)',
                                                     is_peak_hour BOOLEAN COMMENT '是否高峰时段(9-12, 19-21)'
) COMMENT '页面访问明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_visit_detail'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载页面访问数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_visit_detail PARTITION(dt='2025-08-05')
SELECT
    -- 生成唯一访问ID: 页面ID+时间戳(去掉特殊字符)
    CONCAT(page_id, '_', REGEXP_REPLACE(visit_time, '[^0-9]', '')) AS visit_id,
    page_id,
    page_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time, 'yyyy-MM-dd HH:mm:ss')) AS visit_time,
    visitor_count,
    visit_pv,
    click_pv,
    HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time, 'yyyy-MM-dd HH:mm:ss'))) AS hour_of_day,
    CASE
    WHEN HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time))) BETWEEN 9 AND 12 THEN true
    WHEN HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time))) BETWEEN 19 AND 21 THEN true
    ELSE false
END AS is_peak_hour
FROM ods_page_visit_base
WHERE dt = '2025-08-05'
    AND page_id IS NOT NULL
    AND visit_time IS NOT NULL
    AND visit_pv > 0;

select * from dwd_page_visit_detail;

-- 2. 页面板块点击事实表 (dwd_page_block_click_fact)
CREATE TABLE IF NOT EXISTS dwd_page_block_click_fact (
                                                         click_id STRING COMMENT '点击事件ID',
                                                         page_id STRING COMMENT '页面ID',
                                                         block_id STRING COMMENT '板块ID',
                                                         click_pv BIGINT COMMENT '点击量',
                                                         click_uv BIGINT COMMENT '点击人数',
                                                         guide_pay_amount DECIMAL(16,2) COMMENT '引导支付金额',
    collect_time TIMESTAMP COMMENT '采集时间',
    block_category STRING COMMENT '板块分类'
    ) COMMENT '页面板块点击事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_block_click_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载板块点击数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_block_click_fact PARTITION(dt='2025-08-05')
SELECT
    -- 生成唯一点击ID: 页面ID+板块ID+时间戳
    CONCAT(page_id, '_', block_id, '_', REGEXP_REPLACE(collect_time, '[^0-9]', '')) AS click_id,
    page_id,
    block_id,
    click_pv,
    click_uv,
    guide_pay_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(collect_time, 'yyyy-MM-dd HH:mm:ss')) AS collect_time,
    CASE
        WHEN block_name LIKE '%轮播%' THEN '轮播类'
        WHEN block_name LIKE '%推荐%' THEN '推荐类'
        WHEN block_name LIKE '%活动%' THEN '活动类'
        WHEN block_name LIKE '%商品%' THEN '商品类'
        ELSE '其他类'
        END AS block_category
FROM ods_page_block_click
WHERE dt = '2025-08-05'
  AND page_id IS NOT NULL
  AND block_id IS NOT NULL
  AND click_pv > 0  -- 过滤无效点击
  AND guide_pay_amount >= 0;

select * from dwd_page_block_click_fact;

-- 3. 页面数据趋势事实表 (dwd_page_data_trend_fact)
CREATE TABLE IF NOT EXISTS dwd_page_data_trend_fact (
                                                        trend_id STRING COMMENT '趋势记录ID',
                                                        page_id STRING COMMENT '页面ID',
                                                        stat_date DATE COMMENT '统计日期',
                                                        visitor_count BIGINT COMMENT '访客数',
                                                        click_uv BIGINT COMMENT '点击人数',
                                                        data_version STRING COMMENT '数据版本',
                                                        is_current_week BOOLEAN COMMENT '是否本周数据',
                                                        is_weekend BOOLEAN COMMENT '是否周末'
) COMMENT '页面数据趋势事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_data_trend_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载数据趋势 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_data_trend_fact PARTITION(dt='2025-08-05')
SELECT
    CONCAT(page_id, '_', stat_date) AS trend_id,
    page_id,
    TO_DATE(stat_date) AS stat_date,
    visitor_count,
    click_uv,
    data_version,
    -- 判断是否本周（2025-08-05是周二）
    CASE
        WHEN TO_DATE(stat_date) BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05' THEN true
        ELSE false
        END AS is_current_week,
    -- 判断是否周末（周六=6，周日=7）
    CASE
        WHEN DAYOFWEEK(TO_DATE(stat_date)) IN (1,7) THEN true
        ELSE false
        END AS is_weekend
FROM ods_page_data_trend
WHERE dt = '2025-08-05'
  AND page_id IS NOT NULL
  AND stat_date IS NOT NULL
  AND visitor_count >= 0
  AND click_uv >= 0;

select * from dwd_page_data_trend_fact;

-- 4. 页面引导商品事实表 (dwd_page_guide_product_fact)
CREATE TABLE IF NOT EXISTS dwd_page_guide_product_fact (
                                                           guide_id STRING COMMENT '引导记录ID',
                                                           page_id STRING COMMENT '页面ID',
                                                           product_id STRING COMMENT '商品ID',
                                                           guide_count BIGINT COMMENT '引导次数',
                                                           guide_buyer_count BIGINT COMMENT '引导买家数',
                                                           guide_time TIMESTAMP COMMENT '引导时间',
                                                           guide_hour INT COMMENT '引导时段(小时)',
                                                           is_high_value BOOLEAN COMMENT '是否高价值引导(引导买家数>0)'
) COMMENT '页面引导商品事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_guide_product_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载引导商品数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_guide_product_fact PARTITION(dt='2025-08-05')
SELECT
    CONCAT(page_id, '_', product_id, '_', REGEXP_REPLACE(guide_time, '[^0-9]', '')) AS guide_id,
    page_id,
    product_id,
    guide_count,
    guide_buyer_count,
    FROM_UNIXTIME(UNIX_TIMESTAMP(guide_time, 'yyyy-MM-dd HH:mm:ss')) AS guide_time,
    HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(guide_time))) AS guide_hour,
    (guide_buyer_count > 0) AS is_high_value
FROM ods_page_guide_product
WHERE dt = '2025-08-05';

select * from dwd_page_guide_product_fact;

-- 5. 页面模块明细事实表 (dwd_page_module_detail_fact)
CREATE TABLE IF NOT EXISTS dwd_page_module_detail_fact (
                                                           module_id STRING COMMENT '模块ID',
                                                           page_id STRING COMMENT '页面ID',
                                                           module_name STRING COMMENT '模块名称',
                                                           module_type STRING COMMENT '模块类型',
                                                           expose_pv BIGINT COMMENT '曝光量',
                                                           interact_count BIGINT COMMENT '互动量',
                                                           stat_time TIMESTAMP COMMENT '统计时间',
                                                           interaction_rate DECIMAL(5,2) COMMENT '互动率(互动量/曝光量)'
    ) COMMENT '页面模块明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_module_detail_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载模块明细数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_module_detail_fact PARTITION(dt='2025-08-05')
SELECT
    module_id,
    page_id,
    module_name,
    module_type,
    expose_pv,
    interact_count,
    FROM_UNIXTIME(UNIX_TIMESTAMP(stat_time, 'yyyy-MM-dd HH:mm:ss')) AS stat_time,  -- 保持原时间格式转换
    ROUND(COALESCE(interact_count * 1.0 / NULLIF(expose_pv, 0), 0), 4) AS interaction_rate  -- 修复ROUND函数
FROM ods_page_module_detail
WHERE dt = '2025-08-05'
  AND module_id IS NOT NULL
  AND page_id IS NOT NULL
  AND expose_pv > 0  -- 过滤无效曝光
  AND module_type IN ('推荐型', '活动型', '导购型', '内容型');

select * from dwd_page_module_detail_fact;-- 创建DWD层数据库
CREATE DATABASE IF NOT EXISTS gmall_10;
USE gmall_10;

-- 1. 页面访问明细事实表 (dwd_page_visit_detail)
CREATE TABLE IF NOT EXISTS dwd_page_visit_detail (
                                                     visit_id STRING COMMENT '访问唯一ID',
                                                     page_id STRING COMMENT '页面ID',
                                                     page_type STRING COMMENT '页面类型',
                                                     visit_time TIMESTAMP COMMENT '访问时间',
                                                     visitor_count BIGINT COMMENT '访客数',
                                                     visit_pv BIGINT COMMENT '访问量',
                                                     click_pv BIGINT COMMENT '页面总点击量',
                                                     hour_of_day INT COMMENT '小时时段(0-23)',
                                                     is_peak_hour BOOLEAN COMMENT '是否高峰时段(9-12, 19-21)'
) COMMENT '页面访问明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_visit_detail'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载页面访问数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_visit_detail PARTITION(dt='2025-08-05')
SELECT
    -- 生成唯一访问ID: 页面ID+时间戳(去掉特殊字符)
    CONCAT(page_id, '_', REGEXP_REPLACE(visit_time, '[^0-9]', '')) AS visit_id,
    page_id,
    page_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time, 'yyyy-MM-dd HH:mm:ss')) AS visit_time,
    visitor_count,
    visit_pv,
    click_pv,
    HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time, 'yyyy-MM-dd HH:mm:ss'))) AS hour_of_day,
    CASE
    WHEN HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time))) BETWEEN 9 AND 12 THEN true
    WHEN HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(visit_time))) BETWEEN 19 AND 21 THEN true
    ELSE false
END AS is_peak_hour
FROM ods_page_visit_base
WHERE dt = '2025-08-05'
    AND page_id IS NOT NULL
    AND visit_time IS NOT NULL
    AND visit_pv > 0;

select * from dwd_page_visit_detail;

-- 2. 页面板块点击事实表 (dwd_page_block_click_fact)
CREATE TABLE IF NOT EXISTS dwd_page_block_click_fact (
                                                         click_id STRING COMMENT '点击事件ID',
                                                         page_id STRING COMMENT '页面ID',
                                                         block_id STRING COMMENT '板块ID',
                                                         click_pv BIGINT COMMENT '点击量',
                                                         click_uv BIGINT COMMENT '点击人数',
                                                         guide_pay_amount DECIMAL(16,2) COMMENT '引导支付金额',
    collect_time TIMESTAMP COMMENT '采集时间',
    block_category STRING COMMENT '板块分类'
    ) COMMENT '页面板块点击事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_block_click_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载板块点击数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_block_click_fact PARTITION(dt='2025-08-05')
SELECT
    -- 生成唯一点击ID: 页面ID+板块ID+时间戳
    CONCAT(page_id, '_', block_id, '_', REGEXP_REPLACE(collect_time, '[^0-9]', '')) AS click_id,
    page_id,
    block_id,
    click_pv,
    click_uv,
    guide_pay_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(collect_time, 'yyyy-MM-dd HH:mm:ss')) AS collect_time,
    CASE
        WHEN block_name LIKE '%轮播%' THEN '轮播类'
        WHEN block_name LIKE '%推荐%' THEN '推荐类'
        WHEN block_name LIKE '%活动%' THEN '活动类'
        WHEN block_name LIKE '%商品%' THEN '商品类'
        ELSE '其他类'
        END AS block_category
FROM ods_page_block_click
WHERE dt = '2025-08-05'
  AND page_id IS NOT NULL
  AND block_id IS NOT NULL
  AND click_pv > 0  -- 过滤无效点击
  AND guide_pay_amount >= 0;

select * from dwd_page_block_click_fact;

-- 3. 页面数据趋势事实表 (dwd_page_data_trend_fact)
CREATE TABLE IF NOT EXISTS dwd_page_data_trend_fact (
                                                        trend_id STRING COMMENT '趋势记录ID',
                                                        page_id STRING COMMENT '页面ID',
                                                        stat_date DATE COMMENT '统计日期',
                                                        visitor_count BIGINT COMMENT '访客数',
                                                        click_uv BIGINT COMMENT '点击人数',
                                                        data_version STRING COMMENT '数据版本',
                                                        is_current_week BOOLEAN COMMENT '是否本周数据',
                                                        is_weekend BOOLEAN COMMENT '是否周末'
) COMMENT '页面数据趋势事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_data_trend_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载数据趋势 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_data_trend_fact PARTITION(dt='2025-08-05')
SELECT
    CONCAT(page_id, '_', stat_date) AS trend_id,
    page_id,
    TO_DATE(stat_date) AS stat_date,
    visitor_count,
    click_uv,
    data_version,
    -- 判断是否本周（2025-08-05是周二）
    CASE
        WHEN TO_DATE(stat_date) BETWEEN DATE_SUB('2025-08-05', 6) AND '2025-08-05' THEN true
        ELSE false
        END AS is_current_week,
    -- 判断是否周末（周六=6，周日=7）
    CASE
        WHEN DAYOFWEEK(TO_DATE(stat_date)) IN (1,7) THEN true
        ELSE false
        END AS is_weekend
FROM ods_page_data_trend
WHERE dt = '2025-08-05'
  AND page_id IS NOT NULL
  AND stat_date IS NOT NULL
  AND visitor_count >= 0
  AND click_uv >= 0;

select * from dwd_page_data_trend_fact;

-- 4. 页面引导商品事实表 (dwd_page_guide_product_fact)
CREATE TABLE IF NOT EXISTS dwd_page_guide_product_fact (
                                                           guide_id STRING COMMENT '引导记录ID',
                                                           page_id STRING COMMENT '页面ID',
                                                           product_id STRING COMMENT '商品ID',
                                                           guide_count BIGINT COMMENT '引导次数',
                                                           guide_buyer_count BIGINT COMMENT '引导买家数',
                                                           guide_time TIMESTAMP COMMENT '引导时间',
                                                           guide_hour INT COMMENT '引导时段(小时)',
                                                           is_high_value BOOLEAN COMMENT '是否高价值引导(引导买家数>0)'
) COMMENT '页面引导商品事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_guide_product_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载引导商品数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_guide_product_fact PARTITION(dt='2025-08-05')
SELECT
    CONCAT(page_id, '_', product_id, '_', REGEXP_REPLACE(guide_time, '[^0-9]', '')) AS guide_id,
    page_id,
    product_id,
    guide_count,
    guide_buyer_count,
    FROM_UNIXTIME(UNIX_TIMESTAMP(guide_time, 'yyyy-MM-dd HH:mm:ss')) AS guide_time,
    HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(guide_time))) AS guide_hour,
    (guide_buyer_count > 0) AS is_high_value
FROM ods_page_guide_product
WHERE dt = '2025-08-05'
  AND page_id IS NOT NULL
  AND product_id IS NOT NULL
  AND guide_count > 0  -- 过滤无效引导
  AND LENGTH(product_id) = 19;

select * from dwd_page_guide_product_fact;

-- 5. 页面模块明细事实表 (dwd_page_module_detail_fact)
CREATE TABLE IF NOT EXISTS dwd_page_module_detail_fact (
                                                           module_id STRING COMMENT '模块ID',
                                                           page_id STRING COMMENT '页面ID',
                                                           module_name STRING COMMENT '模块名称',
                                                           module_type STRING COMMENT '模块类型',
                                                           expose_pv BIGINT COMMENT '曝光量',
                                                           interact_count BIGINT COMMENT '互动量',
                                                           stat_time TIMESTAMP COMMENT '统计时间',
                                                           interaction_rate DECIMAL(5,2) COMMENT '互动率(互动量/曝光量)'
    ) COMMENT '页面模块明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dwd/dwd_page_module_detail_fact'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并加载模块明细数据 (dt=2025-08-05)
INSERT OVERWRITE TABLE dwd_page_module_detail_fact PARTITION(dt='2025-08-05')
SELECT
    module_id,
    page_id,
    module_name,
    module_type,
    expose_pv,
    interact_count,
    FROM_UNIXTIME(UNIX_TIMESTAMP(stat_time, 'yyyy-MM-dd HH:mm:ss')) AS stat_time,  -- 保持原时间格式转换
    ROUND(COALESCE(interact_count * 1.0 / NULLIF(expose_pv, 0), 0), 4) AS interaction_rate  -- 修复ROUND函数
FROM ods_page_module_detail
WHERE dt = '2025-08-05'
  AND module_id IS NOT NULL
  AND page_id IS NOT NULL
  AND expose_pv > 0;

select * from dwd_page_module_detail_fact;-- 创建DWD层数据库
