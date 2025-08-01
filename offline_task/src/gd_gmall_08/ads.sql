-- 创建ADS数据库

CREATE DATABASE IF NOT EXISTS ds_path;
USE ds_path;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;
-- ===================================================================
-- 1. 设备维度流量统计表 (ADS层)
-- ===================================================================
DROP TABLE IF EXISTS ads_device_traffic_stats;
CREATE TABLE ads_device_traffic_stats (
                                          period_type STRING COMMENT '统计周期(1d/7d/30d)',
                                          device_type STRING COMMENT '设备类型',
                                          session_count BIGINT COMMENT '会话数',
                                          visitor_count BIGINT COMMENT '访客数',
                                          avg_session_duration DOUBLE COMMENT '平均会话时长(秒)',
                                          avg_path_depth DOUBLE COMMENT '平均访问深度',
                                          bounce_rate DOUBLE COMMENT '跳出率',
                                          conversion_rate DOUBLE COMMENT '转化率',
                                          dt STRING COMMENT '统计日期'
) COMMENT '设备维度流量统计表'
STORED AS ORC
location '/warehouse/gd_gmall/ads/ads_device_traffic_stats/';

-- 数据清洗与加载
INSERT OVERWRITE TABLE ads_device_traffic_stats
SELECT
    '1d' AS period_type,
    device_type,
    session_count,
    visitor_count,
    -- 清洗异常值：会话时长小于0时置为0
    CASE WHEN avg_session_duration < 0 THEN 0 ELSE ROUND(avg_session_duration, 2) END,
    -- 清洗异常值：访问深度小于1时置为1
    CASE WHEN avg_path_depth < 1 THEN 1.0 ELSE ROUND(avg_path_depth, 2) END,
    -- 跳出率超过1的修正为1
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    -- 转化率超过1的修正为1
    CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE ROUND(conversion_rate, 4) END,
    dt
FROM ds_path.dws_device_traffic_stats_1d
WHERE dt = '2025-07-31'
  AND device_type IS NOT NULL  -- 过滤空设备类型

UNION ALL

SELECT
    '7d' AS period_type,
    device_type,
    session_count,
    visitor_count,
    CASE WHEN avg_session_duration < 0 THEN 0 ELSE ROUND(avg_session_duration, 2) END,
    CASE WHEN avg_path_depth < 1 THEN 1.0 ELSE ROUND(avg_path_depth, 2) END,
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE ROUND(conversion_rate, 4) END,
    dt
FROM ds_path.dws_device_traffic_stats_7d
WHERE dt = '2025-07-31'
  AND device_type IS NOT NULL

UNION ALL

SELECT
    '30d' AS period_type,
    device_type,
    session_count,
    visitor_count,
    CASE WHEN avg_session_duration < 0 THEN 0 ELSE ROUND(avg_session_duration, 2) END,
    CASE WHEN avg_path_depth < 1 THEN 1.0 ELSE ROUND(avg_path_depth, 2) END,
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE ROUND(conversion_rate, 4) END,
    dt
FROM ds_path.dws_device_traffic_stats_30d
WHERE dt = '2025-07-31'
  AND device_type IS NOT NULL;

select   * from ads_device_traffic_stats;

-- ===================================================================
-- 2. 页面访问统计表 (ADS层)
-- ===================================================================
DROP TABLE IF EXISTS ads_page_visit_stats;
CREATE TABLE ads_page_visit_stats (
                                      period_type STRING COMMENT '统计周期(1d/7d/30d)',
                                      page_id BIGINT COMMENT '页面ID',
                                      page_name STRING COMMENT '页面名称',
                                      page_type STRING COMMENT '页面类型',
                                      device_type STRING COMMENT '设备类型',
                                      visit_count BIGINT COMMENT '访问次数(PV)',
                                      visitor_count BIGINT COMMENT '访客数(UV)',
                                      avg_stay_duration DOUBLE COMMENT '平均停留时长(秒)',
                                      exit_count BIGINT COMMENT '退出次数',
                                      bounce_rate DOUBLE COMMENT '跳出率',
                                      entry_count BIGINT COMMENT '入口次数',
                                      dt STRING COMMENT '统计日期'
) COMMENT '页面访问统计表'
STORED AS ORC
location '/warehouse/gd_gmall/ads/ads_page_visit_stats/';

-- 数据清洗与加载
INSERT OVERWRITE TABLE ads_page_visit_stats
SELECT
    '1d' AS period_type,
    page_id,
    COALESCE(page_name, '未知页面') AS page_name,  -- 空值处理
    COALESCE(page_type, '其他') AS page_type,      -- 空值处理
    COALESCE(device_type, '未知设备') AS device_type,
    visit_count,
    visitor_count,
    -- 停留时长负值处理
    CASE WHEN avg_stay_duration < 0 THEN 0 ELSE ROUND(avg_stay_duration, 2) END,
    exit_count,
    -- 跳出率超限处理
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    entry_count,
    dt
FROM ds_path.dws_page_visit_stats_1d
WHERE dt = '2025-07-31'

UNION ALL

SELECT
    '7d' AS period_type,
    page_id,
    COALESCE(page_name, '未知页面'),
    COALESCE(page_type, '其他'),
    COALESCE(device_type, '未知设备'),
    visit_count,
    visitor_count,
    CASE WHEN avg_stay_duration < 0 THEN 0 ELSE ROUND(avg_stay_duration, 2) END,
    exit_count,
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    entry_count,
    dt
FROM ds_path.dws_page_visit_stats_7d
WHERE dt = '2025-07-31'

UNION ALL

SELECT
    '30d' AS period_type,
    page_id,
    COALESCE(page_name, '未知页面'),
    COALESCE(page_type, '其他'),
    COALESCE(device_type, '未知设备'),
    visit_count,
    visitor_count,
    CASE WHEN avg_stay_duration < 0 THEN 0 ELSE ROUND(avg_stay_duration, 2) END,
    exit_count,
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    entry_count,
    dt
FROM ds_path.dws_page_visit_stats_30d
WHERE dt = '2025-07-31';

select   * from ads_page_visit_stats;

-- ===================================================================
-- 3. 流量入口分析表 (ADS层)
-- ===================================================================
DROP TABLE IF EXISTS ads_traffic_entry_analysis;
CREATE TABLE ads_traffic_entry_analysis (
                                            period_type STRING COMMENT '统计周期(1d/7d/30d)',
                                            entry_page_id BIGINT COMMENT '入口页面ID',
                                            entry_page_name STRING COMMENT '入口页面名称',
                                            entry_page_type STRING COMMENT '入口页面类型',
                                            device_type STRING COMMENT '设备类型',
                                            session_count BIGINT COMMENT '会话数',
                                            visitor_count BIGINT COMMENT '访客数',
                                            avg_session_duration DOUBLE COMMENT '平均会话时长(秒)',
                                            bounce_rate DOUBLE COMMENT '跳出率',
                                            conversion_rate DOUBLE COMMENT '转化率',
                                            dt STRING COMMENT '统计日期'
) COMMENT '流量入口分析表'
STORED AS ORC
LOCATION '/warehouse/gd_gmall/ads/ads_traffic_entry_analysis/';

-- 数据清洗与加载
INSERT OVERWRITE TABLE ads_traffic_entry_analysis
SELECT
    '1d' AS period_type,
    entry_page_id,
    COALESCE(entry_page_name, '未知入口') AS entry_page_name,
    COALESCE(entry_page_type, '其他') AS entry_page_type,
    COALESCE(device_type, '未知设备') AS device_type,
    session_count,
    visitor_count,
    -- 会话时长负值处理
    CASE WHEN avg_session_duration < 0 THEN 0 ELSE ROUND(avg_session_duration, 2) END,
    -- 跳出率超限处理
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    -- 转化率超限处理
    CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE ROUND(conversion_rate, 4) END,
    dt
FROM ds_path.dws_traffic_entry_stats_1d
WHERE dt = '2025-07-31'

UNION ALL

SELECT
    '7d' AS period_type,
    entry_page_id,
    COALESCE(entry_page_name, '未知入口'),
    COALESCE(entry_page_type, '其他'),
    COALESCE(device_type, '未知设备'),
    session_count,
    visitor_count,
    CASE WHEN avg_session_duration < 0 THEN 0 ELSE ROUND(avg_session_duration, 2) END,
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE ROUND(conversion_rate, 4) END,
    dt
FROM ds_path.dws_traffic_entry_stats_7d
WHERE dt = '2025-07-31'

UNION ALL

SELECT
    '30d' AS period_type,
    entry_page_id,
    COALESCE(entry_page_name, '未知入口'),
    COALESCE(entry_page_type, '其他'),
    COALESCE(device_type, '未知设备'),
    session_count,
    visitor_count,
    CASE WHEN avg_session_duration < 0 THEN 0 ELSE ROUND(avg_session_duration, 2) END,
    CASE WHEN bounce_rate > 1.0 THEN 1.0 ELSE ROUND(bounce_rate, 4) END,
    CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE ROUND(conversion_rate, 4) END,
    dt
FROM ds_path.dws_traffic_entry_stats_30d
WHERE dt = '2025-07-31';

select   * from ads_traffic_entry_analysis;

-- ===================================================================
-- 4. 页面路径分析表 (ADS层)
-- ===================================================================
DROP TABLE IF EXISTS ads_page_path_analysis;
CREATE TABLE ads_page_path_analysis (
                                        period_type STRING COMMENT '统计周期(1d/7d/30d)',
                                        from_page_id BIGINT COMMENT '来源页面ID',
                                        from_page_name STRING COMMENT '来源页面名称',
                                        from_page_type STRING COMMENT '来源页面类型',
                                        to_page_id BIGINT COMMENT '目标页面ID',
                                        to_page_name STRING COMMENT '目标页面名称',
                                        to_page_type STRING COMMENT '目标页面类型',
                                        jump_count BIGINT COMMENT '跳转次数',
                                        jump_users BIGINT COMMENT '跳转用户数',
                                        jump_rate DOUBLE COMMENT '跳转率',
                                        drop_off_count BIGINT COMMENT '流失次数',
                                        dt STRING COMMENT '统计日期'
) COMMENT '页面路径分析表'
STORED AS ORC
location '/warehouse/gd_gmall/ads/ads_page_path_analysis/';

-- 数据清洗与加载
INSERT OVERWRITE TABLE ads_page_path_analysis
SELECT
    '1d' AS period_type,
    from_page_id,
    COALESCE(from_page_name, '未知来源') AS from_page_name,
    COALESCE(from_page_type, '其他') AS from_page_type,
    to_page_id,
    COALESCE(to_page_name, '未知目标') AS to_page_name,
    COALESCE(to_page_type, '其他') AS to_page_type,
    jump_count,
    jump_users,
    -- 跳转率超限处理
    CASE WHEN jump_rate > 1.0 THEN 1.0 ELSE ROUND(jump_rate, 4) END,
    drop_off_count,
    dt
FROM ds_path.dws_page_path_flow_1d
WHERE dt = '2025-07-31'

UNION ALL

SELECT
    '7d' AS period_type,
    from_page_id,
    COALESCE(from_page_name, '未知来源'),
    COALESCE(from_page_type, '其他'),
    to_page_id,
    COALESCE(to_page_name, '未知目标'),
    COALESCE(to_page_type, '其他'),
    jump_count,
    jump_users,
    CASE WHEN jump_rate > 1.0 THEN 1.0 ELSE ROUND(jump_rate, 4) END,
    drop_off_count,
    dt
FROM ds_path.dws_page_path_flow_7d
WHERE dt = '2025-07-31'

UNION ALL

SELECT
    '30d' AS period_type,
    from_page_id,
    COALESCE(from_page_name, '未知来源'),
    COALESCE(from_page_type, '其他'),
    to_page_id,
    COALESCE(to_page_name, '未知目标'),
    COALESCE(to_page_type, '其他'),
    jump_count,
    jump_users,
    CASE WHEN jump_rate > 1.0 THEN 1.0 ELSE ROUND(jump_rate, 4) END,
    drop_off_count,
    dt
FROM ds_path.dws_page_path_flow_30d
WHERE dt = '2025-07-31';

select   * from ads_page_path_analysis;