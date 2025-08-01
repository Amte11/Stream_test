SET hive.exec.mode.local.auto=true;
USE ds_path;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;
-- ===================================================================
-- 1. 设备维度流量统计表 (支持无线端/PC端数据)
-- ===================================================================

-- 1.1 1天数据表
DROP TABLE IF EXISTS dws_device_traffic_stats_1d;
CREATE TABLE dws_device_traffic_stats_1d (
                                             device_type STRING COMMENT '设备类型',
                                             session_count BIGINT COMMENT '会话数',
                                             visitor_count BIGINT COMMENT '访客数',
                                             avg_session_duration DOUBLE COMMENT '平均会话时长',
                                             avg_path_depth DOUBLE COMMENT '平均访问深度',
                                             bounce_rate DOUBLE COMMENT '跳出率',
                                             conversion_rate DOUBLE COMMENT '转化率'
)
    COMMENT '设备维度流量统计表(1天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_device_traffic_stats_1d/';

INSERT OVERWRITE TABLE dws_device_traffic_stats_1d PARTITION(dt='2025-07-31')
SELECT
    sp.device_type,
    COUNT(sp.session_id) AS session_count,
    COUNT(DISTINCT sp.user_id) AS visitor_count,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    ROUND(AVG(sp.path_depth), 2) AS avg_path_depth,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN uc.is_converted = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS conversion_rate
FROM dwd_session_path sp
         LEFT JOIN dwd_user_conversion uc
                   ON sp.session_id = uc.session_id AND uc.dt='2025-07-31'
WHERE sp.dt='2025-07-31'
GROUP BY sp.device_type;

select * from dws_device_traffic_stats_1d;

-- 1.2 7天数据表
DROP TABLE IF EXISTS dws_device_traffic_stats_7d;
CREATE TABLE dws_device_traffic_stats_7d (
                                             device_type STRING COMMENT '设备类型',
                                             session_count BIGINT COMMENT '会话数',
                                             visitor_count BIGINT COMMENT '访客数',
                                             avg_session_duration DOUBLE COMMENT '平均会话时长',
                                             avg_path_depth DOUBLE COMMENT '平均访问深度',
                                             bounce_rate DOUBLE COMMENT '跳出率',
                                             conversion_rate DOUBLE COMMENT '转化率'
)
    COMMENT '设备维度流量统计表(7天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_device_traffic_stats_7d/';

INSERT INTO TABLE dws_device_traffic_stats_7d PARTITION(dt='2025-07-31')
SELECT
    sp.device_type,
    COUNT(sp.session_id) AS session_count,
    COUNT(DISTINCT sp.user_id) AS visitor_count,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    ROUND(AVG(sp.path_depth), 2) AS avg_path_depth,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN uc.is_converted = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS conversion_rate
FROM (
         SELECT * FROM dwd_session_path
         WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
     ) sp
         LEFT JOIN (
    SELECT * FROM dwd_user_conversion
    WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
) uc ON sp.session_id = uc.session_id
GROUP BY sp.device_type;

select * from dws_device_traffic_stats_7d;

-- 1.3 30天数据表
DROP TABLE IF EXISTS dws_device_traffic_stats_30d;
CREATE TABLE dws_device_traffic_stats_30d (
                                              device_type STRING COMMENT '设备类型',
                                              session_count BIGINT COMMENT '会话数',
                                              visitor_count BIGINT COMMENT '访客数',
                                              avg_session_duration DOUBLE COMMENT '平均会话时长',
                                              avg_path_depth DOUBLE COMMENT '平均访问深度',
                                              bounce_rate DOUBLE COMMENT '跳出率',
                                              conversion_rate DOUBLE COMMENT '转化率'
)
    COMMENT '设备维度流量统计表(30天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_device_traffic_stats_30d/';

INSERT INTO TABLE dws_device_traffic_stats_30d PARTITION(dt='2025-07-31')
SELECT
    sp.device_type,
    COUNT(sp.session_id) AS session_count,
    COUNT(DISTINCT sp.user_id) AS visitor_count,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    ROUND(AVG(sp.path_depth), 2) AS avg_path_depth,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN uc.is_converted = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS conversion_rate
FROM (
         SELECT * FROM dwd_session_path
         WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
     ) sp
         LEFT JOIN (
    SELECT * FROM dwd_user_conversion
    WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
) uc ON sp.session_id = uc.session_id
GROUP BY sp.device_type ;

select * from dws_device_traffic_stats_30d;
-- ===================================================================
-- 2. 页面访问统计汇总表 (支持页面排行)
-- ===================================================================

-- 2.1 1天数据表
DROP TABLE IF EXISTS dws_page_visit_stats_1d;
CREATE TABLE dws_page_visit_stats_1d (
                                         page_id BIGINT COMMENT '页面ID',
                                         page_name STRING COMMENT '页面名称',
                                         page_type STRING COMMENT '页面类型',
                                         device_type STRING COMMENT '设备类型',
                                         visit_count BIGINT COMMENT '访问次数(PV)',
                                         visitor_count BIGINT COMMENT '访客数(UV)',
                                         avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                         exit_count BIGINT COMMENT '退出次数',
                                         bounce_rate DOUBLE COMMENT '跳出率',
                                         entry_count BIGINT COMMENT '入口次数'
)
    COMMENT '页面访问统计汇总表(1天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_visit_stats_1d/';

INSERT OVERWRITE TABLE dws_page_visit_stats_1d PARTITION(dt='2025-07-31')
SELECT
    pvd.page_id,
    pvd.page_name,
    pvd.page_type,
    pvd.device_type,
    COUNT(1) AS visit_count,
    COUNT(DISTINCT pvd.user_id) AS visitor_count,
    ROUND(AVG(pvd.stay_duration), 2) AS avg_stay_duration,
    SUM(CASE WHEN sp.exit_page_id = pvd.page_id THEN 1 ELSE 0 END) AS exit_count,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    SUM(CASE WHEN sp.entry_page_id = pvd.page_id THEN 1 ELSE 0 END) AS entry_count
FROM dwd_page_visit_detail pvd
         JOIN dwd_session_path sp
              ON pvd.session_id = sp.session_id AND sp.dt='2025-07-31'
WHERE pvd.dt='2025-07-31'
GROUP BY pvd.page_id, pvd.page_name, pvd.page_type, pvd.device_type;


select * from dws_page_visit_stats_1d;
-- 2.2 7天数据表
DROP TABLE IF EXISTS dws_page_visit_stats_7d;
CREATE TABLE dws_page_visit_stats_7d (
                                         page_id BIGINT COMMENT '页面ID',
                                         page_name STRING COMMENT '页面名称',
                                         page_type STRING COMMENT '页面类型',
                                         device_type STRING COMMENT '设备类型',
                                         visit_count BIGINT COMMENT '访问次数(PV)',
                                         visitor_count BIGINT COMMENT '访客数(UV)',
                                         avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                         exit_count BIGINT COMMENT '退出次数',
                                         bounce_rate DOUBLE COMMENT '跳出率',
                                         entry_count BIGINT COMMENT '入口次数'
)
    COMMENT '页面访问统计汇总表(7天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_visit_stats_7d/';

INSERT INTO TABLE dws_page_visit_stats_7d PARTITION(dt='2025-07-31')
SELECT
    pvd.page_id,
    pvd.page_name,
    pvd.page_type,
    pvd.device_type,
    COUNT(1) AS visit_count,
    COUNT(DISTINCT pvd.user_id) AS visitor_count,
    ROUND(AVG(pvd.stay_duration), 2) AS avg_stay_duration,
    SUM(CASE WHEN sp.exit_page_id = pvd.page_id THEN 1 ELSE 0 END) AS exit_count,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    SUM(CASE WHEN sp.entry_page_id = pvd.page_id THEN 1 ELSE 0 END) AS entry_count
FROM (
         SELECT * FROM dwd_page_visit_detail
         WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
     ) pvd
         JOIN (
    SELECT * FROM dwd_session_path
    WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
) sp ON pvd.session_id = sp.session_id
GROUP BY pvd.page_id, pvd.page_name, pvd.page_type, pvd.device_type;

select * from dws_page_visit_stats_7d;
-- 2.3 30天数据表
DROP TABLE IF EXISTS dws_page_visit_stats_30d;
CREATE TABLE dws_page_visit_stats_30d (
                                          page_id BIGINT COMMENT '页面ID',
                                          page_name STRING COMMENT '页面名称',
                                          page_type STRING COMMENT '页面类型',
                                          device_type STRING COMMENT '设备类型',
                                          visit_count BIGINT COMMENT '访问次数(PV)',
                                          visitor_count BIGINT COMMENT '访客数(UV)',
                                          avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                          exit_count BIGINT COMMENT '退出次数',
                                          bounce_rate DOUBLE COMMENT '跳出率',
                                          entry_count BIGINT COMMENT '入口次数'
)
    COMMENT '页面访问统计汇总表(30天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_visit_stats_30d/';

INSERT INTO TABLE dws_page_visit_stats_30d PARTITION(dt='2025-07-31')
SELECT
    pvd.page_id,
    pvd.page_name,
    pvd.page_type,
    pvd.device_type,
    COUNT(1) AS visit_count,
    COUNT(DISTINCT pvd.user_id) AS visitor_count,
    ROUND(AVG(pvd.stay_duration), 2) AS avg_stay_duration,
    SUM(CASE WHEN sp.exit_page_id = pvd.page_id THEN 1 ELSE 0 END) AS exit_count,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    SUM(CASE WHEN sp.entry_page_id = pvd.page_id THEN 1 ELSE 0 END) AS entry_count
FROM (
         SELECT * FROM dwd_page_visit_detail
         WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
     ) pvd
         JOIN (
    SELECT * FROM dwd_session_path
    WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
) sp ON pvd.session_id = sp.session_id
GROUP BY pvd.page_id, pvd.page_name, pvd.page_type, pvd.device_type;

select * from dws_page_visit_stats_30d;
-- ===================================================================
-- 3. 流量入口统计表 (支持无线端数据)
-- ===================================================================

-- 3.1 1天数据表
DROP TABLE IF EXISTS dws_traffic_entry_stats_1d;
CREATE TABLE dws_traffic_entry_stats_1d (
                                            entry_page_id BIGINT COMMENT '入口页面ID',
                                            entry_page_name STRING COMMENT '入口页面名称',
                                            entry_page_type STRING COMMENT '入口页面类型',
                                            device_type STRING COMMENT '设备类型',
                                            session_count BIGINT COMMENT '会话数',
                                            visitor_count BIGINT COMMENT '访客数',
                                            avg_session_duration DOUBLE COMMENT '平均会话时长',
                                            bounce_rate DOUBLE COMMENT '跳出率',
                                            conversion_rate DOUBLE COMMENT '转化率'
)
    COMMENT '流量入口统计表(1天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_traffic_entry_stats_1d/';

INSERT OVERWRITE TABLE dws_traffic_entry_stats_1d PARTITION(dt='2025-07-31')
SELECT
    sp.entry_page_id,
    pvd.page_name AS entry_page_name,
    pvd.page_type AS entry_page_type,
    sp.device_type,
    COUNT(sp.session_id) AS session_count,
    COUNT(DISTINCT sp.user_id) AS visitor_count,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN uc.is_converted = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS conversion_rate
FROM dwd_session_path sp
         LEFT JOIN dwd_user_conversion uc
                   ON sp.session_id = uc.session_id AND uc.dt='2025-07-31'
         JOIN dwd_page_visit_detail pvd
              ON sp.entry_page_id = pvd.page_id AND pvd.dt='2025-07-31'
WHERE sp.dt='2025-07-31'
GROUP BY sp.entry_page_id, pvd.page_name, pvd.page_type, sp.device_type;

select * from dws_traffic_entry_stats_1d;
-- 3.2 7天数据表
DROP TABLE IF EXISTS dws_traffic_entry_stats_7d;
CREATE TABLE dws_traffic_entry_stats_7d (
                                            entry_page_id BIGINT COMMENT '入口页面ID',
                                            entry_page_name STRING COMMENT '入口页面名称',
                                            entry_page_type STRING COMMENT '入口页面类型',
                                            device_type STRING COMMENT '设备类型',
                                            session_count BIGINT COMMENT '会话数',
                                            visitor_count BIGINT COMMENT '访客数',
                                            avg_session_duration DOUBLE COMMENT '平均会话时长',
                                            bounce_rate DOUBLE COMMENT '跳出率',
                                            conversion_rate DOUBLE COMMENT '转化率'
)
    COMMENT '流量入口统计表(7天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_traffic_entry_stats_7d/';

INSERT INTO TABLE dws_traffic_entry_stats_7d PARTITION(dt='2025-07-31')
SELECT
    sp.entry_page_id,
    pvd.page_name AS entry_page_name,
    pvd.page_type AS entry_page_type,
    sp.device_type,
    COUNT(sp.session_id) AS session_count,
    COUNT(DISTINCT sp.user_id) AS visitor_count,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN uc.is_converted = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS conversion_rate
FROM (
         SELECT * FROM dwd_session_path
         WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
     ) sp
         LEFT JOIN (
    SELECT * FROM dwd_user_conversion
    WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
) uc ON sp.session_id = uc.session_id
         JOIN (
    SELECT * FROM dwd_page_visit_detail
    WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
) pvd ON sp.entry_page_id = pvd.page_id
GROUP BY sp.entry_page_id, pvd.page_name, pvd.page_type, sp.device_type;

select * from dws_traffic_entry_stats_7d;
-- 3.3 30天数据表
DROP TABLE IF EXISTS dws_traffic_entry_stats_30d;
CREATE TABLE dws_traffic_entry_stats_30d (
                                             entry_page_id BIGINT COMMENT '入口页面ID',
                                             entry_page_name STRING COMMENT '入口页面名称',
                                             entry_page_type STRING COMMENT '入口页面类型',
                                             device_type STRING COMMENT '设备类型',
                                             session_count BIGINT COMMENT '会话数',
                                             visitor_count BIGINT COMMENT '访客数',
                                             avg_session_duration DOUBLE COMMENT '平均会话时长',
                                             bounce_rate DOUBLE COMMENT '跳出率',
                                             conversion_rate DOUBLE COMMENT '转化率'
)
    COMMENT '流量入口统计表(30天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_traffic_entry_stats_30d/';

INSERT INTO TABLE dws_traffic_entry_stats_30d PARTITION(dt='2025-07-31')
SELECT
    sp.entry_page_id,
    pvd.page_name AS entry_page_name,
    pvd.page_type AS entry_page_type,
    sp.device_type,
    COUNT(sp.session_id) AS session_count,
    COUNT(DISTINCT sp.user_id) AS visitor_count,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    ROUND(SUM(CASE WHEN sp.path_depth = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN uc.is_converted = 1 THEN 1 ELSE 0 END) / COUNT(1), 4) AS conversion_rate
FROM (
         SELECT * FROM dwd_session_path
         WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
     ) sp
         LEFT JOIN (
    SELECT * FROM dwd_user_conversion
    WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
) uc ON sp.session_id = uc.session_id
         JOIN (
    SELECT * FROM dwd_page_visit_detail
    WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
) pvd ON sp.entry_page_id = pvd.page_id
GROUP BY sp.entry_page_id, pvd.page_name, pvd.page_type, sp.device_type;

select * from dws_traffic_entry_stats_30d;

-- ===================================================================
-- 4. 页面路径流转表 (支持路径分析)
-- ===================================================================
-- 4.1 1天数据表
DROP TABLE IF EXISTS dws_page_path_flow_1d;
CREATE TABLE dws_page_path_flow_1d (
                                       from_page_id BIGINT COMMENT '来源页面ID',
                                       from_page_name STRING COMMENT '来源页面名称',
                                       from_page_type STRING COMMENT '来源页面类型',
                                       to_page_id BIGINT COMMENT '目标页面ID',
                                       to_page_name STRING COMMENT '目标页面名称',
                                       to_page_type STRING COMMENT '目标页面类型',
                                       jump_count BIGINT COMMENT '跳转次数',
                                       jump_users BIGINT COMMENT '跳转用户数',
                                       jump_rate DOUBLE COMMENT '跳转率',
                                       drop_off_count BIGINT COMMENT '流失次数'
)
    COMMENT '页面路径流转表(1天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_path_flow_1d/';

-- 设置 MapReduce 任务内存（根据实际情况调整，可在执行 SQL 前设置）
INSERT OVERWRITE TABLE dws_page_path_flow_1d PARTITION(dt='2025-07-31')
SELECT
    pj.from_page_id,
    MAX(f_page.page_name) AS from_page_name,
    MAX(f_page.page_type) AS from_page_type,
    pj.to_page_id,
    MAX(t_page.page_name) AS to_page_name,
    MAX(t_page.page_type) AS to_page_type,
    SUM(pj.jump_count) AS jump_count,
    COUNT(DISTINCT pj.jump_users) AS jump_users,
    ROUND(
            CASE WHEN MAX(f_stats.visit_count) = 0 THEN 0
                 ELSE SUM(pj.jump_count)*1.0/MAX(f_stats.visit_count)
                END, 4
        ) AS jump_rate,
    MAX(f_stats.exit_count) AS drop_off_count
FROM dwd_page_jump pj
         JOIN (
    SELECT
        page_id,
        SUM(visit_count) AS visit_count,
        SUM(exit_count) AS exit_count
    FROM dws_page_visit_stats_1d
    WHERE dt = '2025-07-31'
    GROUP BY page_id
) f_stats ON pj.from_page_id = f_stats.page_id
         JOIN (
    SELECT
        page_id,
        page_name,
        page_type,
        ROW_NUMBER() OVER(PARTITION BY page_id ORDER BY dt DESC) AS rn
    FROM dwd_page_visit_detail
    WHERE dt = '2025-07-31'
) f_page ON pj.from_page_id = f_page.page_id AND f_page.rn = 1
         JOIN (
    SELECT
        page_id,
        page_name,
        page_type,
        ROW_NUMBER() OVER(PARTITION BY page_id ORDER BY dt DESC) AS rn
    FROM dwd_page_visit_detail
    WHERE dt = '2025-07-31'
) t_page ON pj.to_page_id = t_page.page_id AND t_page.rn = 1
WHERE pj.dt = '2025-07-31'
GROUP BY pj.from_page_id, pj.to_page_id;

select * from dws_page_path_flow_1d;

-- 4.2 7天数据表
DROP TABLE IF EXISTS dws_page_path_flow_7d;
CREATE TABLE dws_page_path_flow_7d (
                                       from_page_id BIGINT COMMENT '来源页面ID',
                                       from_page_name STRING COMMENT '来源页面名称',
                                       from_page_type STRING COMMENT '来源页面类型',
                                       to_page_id BIGINT COMMENT '目标页面ID',
                                       to_page_name STRING COMMENT '目标页面名称',
                                       to_page_type STRING COMMENT '目标页面类型',
                                       jump_count BIGINT COMMENT '跳转次数',
                                       jump_users BIGINT COMMENT '跳转用户数',
                                       jump_rate DOUBLE COMMENT '跳转率',
                                       drop_off_count BIGINT COMMENT '流失次数'
)
    COMMENT '页面路径流转表(7天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_path_flow_7d/';

INSERT OVERWRITE TABLE dws_page_path_flow_7d PARTITION(dt='2025-07-31')
SELECT
    pj.from_page_id,
    MAX(f_pvd.page_name) AS from_page_name,
    MAX(f_pvd.page_type) AS from_page_type,
    pj.to_page_id,
    MAX(t_pvd.page_name) AS to_page_name,
    MAX(t_pvd.page_type) AS to_page_type,
    SUM(pj.jump_count) AS jump_count,
    COUNT(DISTINCT pj.jump_users) AS jump_users,
    ROUND(SUM(pj.jump_count) * 1.0 / NULLIF(MAX(f_stats.visit_count), 0), 4) AS jump_rate,
    MAX(f_stats.exit_count) AS drop_off_count
FROM (
         SELECT * FROM dwd_page_jump
         WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
     ) pj
         JOIN dws_page_visit_stats_7d f_stats
              ON pj.from_page_id = f_stats.page_id
                  AND f_stats.dt = '2025-07-31'
         JOIN (
    SELECT DISTINCT page_id, page_name, page_type
    FROM dwd_page_visit_detail
    WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
) f_pvd ON pj.from_page_id = f_pvd.page_id
         JOIN (
    SELECT DISTINCT page_id, page_name, page_type
    FROM dwd_page_visit_detail
    WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
) t_pvd ON pj.to_page_id = t_pvd.page_id
GROUP BY pj.from_page_id, pj.to_page_id;

select * from dws_page_path_flow_7d;

-- 4.3 30天数据表
DROP TABLE IF EXISTS dws_page_path_flow_30d;
CREATE TABLE dws_page_path_flow_30d (
                                        from_page_id BIGINT COMMENT '来源页面ID',
                                        from_page_name STRING COMMENT '来源页面名称',
                                        from_page_type STRING COMMENT '来源页面类型',
                                        to_page_id BIGINT COMMENT '目标页面ID',
                                        to_page_name STRING COMMENT '目标页面名称',
                                        to_page_type STRING COMMENT '目标页面类型',
                                        jump_count BIGINT COMMENT '跳转次数',
                                        jump_users BIGINT COMMENT '跳转用户数',
                                        jump_rate DOUBLE COMMENT '跳转率',
                                        drop_off_count BIGINT COMMENT '流失次数'
)
    COMMENT '页面路径流转表(30天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_path_flow_30d/';

INSERT OVERWRITE TABLE dws_page_path_flow_30d PARTITION(dt='2025-07-31')
SELECT
    pj.from_page_id,
    MAX(f_pvd.page_name) AS from_page_name,
    MAX(f_pvd.page_type) AS from_page_type,
    pj.to_page_id,
    MAX(t_pvd.page_name) AS to_page_name,
    MAX(t_pvd.page_type) AS to_page_type,
    SUM(pj.jump_count) AS jump_count,
    COUNT(DISTINCT pj.jump_users) AS jump_users,
    ROUND(SUM(pj.jump_count) * 1.0 / NULLIF(MAX(f_stats.visit_count), 0), 4) AS jump_rate,
    MAX(f_stats.exit_count) AS drop_off_count
FROM (
         SELECT * FROM dwd_page_jump
         WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
     ) pj
         JOIN dws_page_visit_stats_30d f_stats
              ON pj.from_page_id = f_stats.page_id
                  AND f_stats.dt = '2025-07-31'
         JOIN (
    SELECT DISTINCT page_id, page_name, page_type
    FROM dwd_page_visit_detail
    WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
) f_pvd ON pj.from_page_id = f_pvd.page_id
         JOIN (
    SELECT DISTINCT page_id, page_name, page_type
    FROM dwd_page_visit_detail
    WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
) t_pvd ON pj.to_page_id = t_pvd.page_id
GROUP BY pj.from_page_id, pj.to_page_id;

select * from dws_page_path_flow_30d;

-- ===================================================================
-- 5. 用户路径行为分析表 (支持转化分析)
-- ===================================================================
-- 5.1 1天数据表
DROP TABLE IF EXISTS dws_user_path_behavior_1d;
CREATE TABLE dws_user_path_behavior_1d (
                                           user_id BIGINT COMMENT '用户ID',
                                           session_count BIGINT COMMENT '会话数',
                                           total_page_views BIGINT COMMENT '总页面浏览数',
                                           total_duration BIGINT COMMENT '总停留时长',
                                           avg_session_duration DOUBLE COMMENT '平均会话时长',
                                           conversion_count BIGINT COMMENT '转化次数',
                                           total_conversion_value DECIMAL(18,2) COMMENT '总转化价值',
                                           entry_page_distribution MAP<STRING, string> COMMENT '入口页面分布'
)
    COMMENT '用户路径行为分析表(1天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_user_path_behavior_1d/';

INSERT OVERWRITE TABLE dws_user_path_behavior_1d PARTITION(dt='2025-07-31')
SELECT
    sp.user_id,
    COUNT(DISTINCT sp.session_id) AS session_count,
    SUM(sp.path_depth) AS total_page_views,
    SUM(sp.total_duration) AS total_duration,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    COUNT(DISTINCT uc.session_id) AS conversion_count,
    COALESCE(SUM(uc.conversion_value), 0) AS total_conversion_value,
    STR_TO_MAP(
            CONCAT_WS(',', COLLECT_LIST(CONCAT(entry_page, ':', CAST(cnt AS STRING))), ','),
            ':'
        ) AS entry_page_distribution
FROM dwd_session_path sp
         LEFT JOIN dwd_user_conversion uc
                   ON sp.session_id = uc.session_id
                       AND uc.dt = '2025-07-31'
         JOIN (
    SELECT
        session_id,
        page_name AS entry_page,
        COUNT(*) AS cnt
    FROM (
             SELECT
                 session_id,
                 page_name,
                 ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time) AS rn
             FROM dwd_page_visit_detail
             WHERE dt = '2025-07-31'
         ) t
    WHERE rn = 1
    GROUP BY session_id, page_name
) entry ON sp.session_id = entry.session_id
WHERE sp.dt = '2025-07-31'
GROUP BY sp.user_id;

select  * from dws_user_path_behavior_1d;

-- 5.2 7天数据表
DROP TABLE IF EXISTS dws_user_path_behavior_7d;
CREATE TABLE dws_user_path_behavior_7d (
                                           user_id BIGINT COMMENT '用户ID',
                                           session_count BIGINT COMMENT '会话数',
                                           total_page_views BIGINT COMMENT '总页面浏览数',
                                           total_duration BIGINT COMMENT '总停留时长',
                                           avg_session_duration DOUBLE COMMENT '平均会话时长',
                                           conversion_count BIGINT COMMENT '转化次数',
                                           total_conversion_value DECIMAL(18,2) COMMENT '总转化价值',
                                           entry_page_distribution MAP<STRING, string> COMMENT '入口页面分布'
)
    COMMENT '用户路径行为分析表(7天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_user_path_behavior_7d/';

INSERT OVERWRITE TABLE dws_user_path_behavior_7d PARTITION(dt='2025-07-31')
SELECT
    sp.user_id,
    COUNT(DISTINCT sp.session_id) AS session_count,
    SUM(sp.path_depth) AS total_page_views,
    SUM(sp.total_duration) AS total_duration,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    COUNT(DISTINCT uc.session_id) AS conversion_count,
    COALESCE(SUM(uc.conversion_value), 0) AS total_conversion_value,
    STR_TO_MAP(
            CONCAT_WS(',', COLLECT_LIST(CONCAT(entry_page, ':', CAST(cnt AS STRING)))),
            ',',
            ':'
        ) AS entry_page_distribution
FROM (
         SELECT * FROM dwd_session_path
         WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
     ) sp
         LEFT JOIN dwd_user_conversion uc
                   ON sp.session_id = uc.session_id
                       AND uc.dt BETWEEN '2025-07-25' AND '2025-07-31'
         JOIN (
    SELECT
        session_id,
        page_name AS entry_page,
        COUNT(*) AS cnt
    FROM (
             SELECT
                 session_id,
                 page_name,
                 ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time) AS rn
             FROM dwd_page_visit_detail
             WHERE dt BETWEEN '2025-07-25' AND '2025-07-31'
         ) t
    WHERE rn = 1
    GROUP BY session_id, page_name
) entry ON sp.session_id = entry.session_id
GROUP BY sp.user_id;

select * from dws_user_path_behavior_7d;
-- 5.3 30天数据表
DROP TABLE IF EXISTS dws_user_path_behavior_30d;
CREATE TABLE dws_user_path_behavior_30d (
                                            user_id BIGINT COMMENT '用户ID',
                                            session_count BIGINT COMMENT '会话数',
                                            total_page_views BIGINT COMMENT '总页面浏览数',
                                            total_duration BIGINT COMMENT '总停留时长',
                                            avg_session_duration DOUBLE COMMENT '平均会话时长',
                                            conversion_count BIGINT COMMENT '转化次数',
                                            total_conversion_value DECIMAL(18,2) COMMENT '总转化价值',
                                            entry_page_distribution MAP<STRING, string> COMMENT '入口页面分布'
)
    COMMENT '用户路径行为分析表(30天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_user_path_behavior_30d/';

INSERT OVERWRITE TABLE dws_user_path_behavior_30d PARTITION(dt='2025-07-31')
SELECT
    sp.user_id,
    COUNT(DISTINCT sp.session_id) AS session_count,
    SUM(sp.path_depth) AS total_page_views,
    SUM(sp.total_duration) AS total_duration,
    ROUND(AVG(sp.total_duration), 2) AS avg_session_duration,
    COUNT(DISTINCT uc.session_id) AS conversion_count,
    COALESCE(SUM(uc.conversion_value), 0) AS total_conversion_value,
    STR_TO_MAP(
            CONCAT_WS(',', COLLECT_LIST(CONCAT(entry_page, ':', CAST(cnt AS STRING)))),
            ',',
            ':'
        ) AS entry_page_distribution
FROM (
         SELECT * FROM dwd_session_path
         WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
     ) sp
         LEFT JOIN dwd_user_conversion uc
                   ON sp.session_id = uc.session_id
                       AND uc.dt BETWEEN '2025-07-02' AND '2025-07-31'
         JOIN (
    SELECT
        session_id,
        page_name AS entry_page,
        COUNT(*) AS cnt
    FROM (
             SELECT
                 session_id,
                 page_name,
                 ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time) AS rn
             FROM dwd_page_visit_detail
             WHERE dt BETWEEN '2025-07-02' AND '2025-07-31'
         ) t
    WHERE rn = 1
    GROUP BY session_id, page_name
) entry ON sp.session_id = entry.session_id
GROUP BY sp.user_id;

select * from dws_user_path_behavior_30d;

-- ===================================================================
-- 6. 页面类型汇总表 (支持页面类型排行)
-- ===================================================================
-- 6.1 1天数据表
-- 1. 1天数据表 (修复跳出率计算)
-- 1. 1天数据表 (修复跳出率计算 + 无is_last_page字段的兼容方案)
DROP TABLE IF EXISTS dws_page_type_summary_1d;
CREATE TABLE dws_page_type_summary_1d (
                                          page_type STRING COMMENT '页面类型',
                                          device_type STRING COMMENT '设备类型',
                                          visit_count BIGINT COMMENT '访问次数',
                                          visitor_count BIGINT COMMENT '访客数',
                                          avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                          exit_count BIGINT COMMENT '退出次数',
                                          bounce_rate DOUBLE COMMENT '跳出率',
                                          conversion_count BIGINT COMMENT '转化次数'
)
    COMMENT '页面类型汇总表(1天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_type_summary_1d/';

INSERT OVERWRITE TABLE dws_page_type_summary_1d PARTITION(dt='2025-07-31')
SELECT
    pvd.page_type,
    pvd.device_type,
    COUNT(*) AS visit_count,
    COUNT(DISTINCT pvd.user_id) AS visitor_count,
    ROUND(AVG(pvd.stay_duration), 2) AS avg_stay_duration,
    -- 替换is_last_page：通过会话内页面顺序判断最后一页
    SUM(CASE WHEN pvd.rn = pvd.max_rn THEN 1 ELSE 0 END) AS exit_count,
    ROUND(
                    COUNT(DISTINCT CASE WHEN sp.path_depth = 1 THEN sp.session_id END) * 1.0
                / COUNT(DISTINCT sp.session_id),
                    4
        ) AS bounce_rate,
    COUNT(DISTINCT CASE WHEN uc.is_converted = 1 THEN uc.session_id END) AS conversion_count
FROM (
         -- 子查询：为每个会话内的页面标记访问顺序（rn）和最大rn（max_rn）
         SELECT
             *,
             ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time ASC) AS rn,  -- 会话内页面访问顺序
                 MAX(ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time ASC))
                     OVER (PARTITION BY session_id) AS max_rn  -- 会话内最大rn（最后一页标记）
         FROM dwd_page_visit_detail
         WHERE dt = '2025-07-31'
     ) pvd
         JOIN dwd_session_path sp
              ON pvd.session_id = sp.session_id AND sp.dt = '2025-07-31'
         LEFT JOIN dwd_user_conversion uc
                   ON pvd.session_id = uc.session_id AND uc.dt = '2025-07-31'
GROUP BY
    pvd.page_type,
    pvd.device_type;


SELECT * FROM dws_page_type_summary_1d;

-- 2. 7天数据表（同理优化）
DROP TABLE IF EXISTS dws_page_type_summary_7d;
CREATE TABLE dws_page_type_summary_7d (
                                          page_type STRING COMMENT '页面类型',
                                          device_type STRING COMMENT '设备类型',
                                          visit_count BIGINT COMMENT '访问次数',
                                          visitor_count BIGINT COMMENT '访客数',
                                          avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                          exit_count BIGINT COMMENT '退出次数',
                                          bounce_rate DOUBLE COMMENT '跳出率',
                                          conversion_count BIGINT COMMENT '转化次数'
)
    COMMENT '页面类型汇总表(7天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_type_summary_7d/';

INSERT OVERWRITE TABLE dws_page_type_summary_7d PARTITION(dt='2025-07-31')
SELECT
    pvd.page_type,
    pvd.device_type,
    COUNT(*) AS visit_count,
    COUNT(DISTINCT pvd.user_id) AS visitor_count,
    ROUND(AVG(pvd.stay_duration), 2) AS avg_stay_duration,
    SUM(CASE WHEN pvd.is_last_page = 1 THEN 1 ELSE 0 END) AS exit_count,
    ROUND(
                    COUNT(DISTINCT CASE WHEN sp.page_count = 1 THEN sp.session_id END) * 1.0
                / COUNT(DISTINCT sp.session_id),
                    4
        ) AS bounce_rate,
    SUM(uc.is_converted) AS conversion_count
FROM (
         SELECT
             *,
             ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time DESC) = 1 AS is_last_page
         FROM dwd_page_visit_detail
         WHERE dt BETWEEN DATE_SUB('2025-07-31', 6) AND '2025-07-31'  -- 动态计算7天范围
     ) pvd
         JOIN (
    SELECT session_id, COUNT(*) AS page_count
    FROM dwd_page_visit_detail
    WHERE dt BETWEEN DATE_SUB('2025-07-31', 6) AND '2025-07-31'
    GROUP BY session_id
) sp ON pvd.session_id = sp.session_id
         LEFT JOIN dwd_user_conversion uc
                   ON pvd.session_id = uc.session_id
                       AND uc.dt BETWEEN DATE_SUB('2025-07-31', 6) AND '2025-07-31'
GROUP BY pvd.page_type, pvd.device_type;

SELECT * FROM dws_page_type_summary_7d;

-- 3. 30天数据表（优化逻辑同7d）
DROP TABLE IF EXISTS dws_page_type_summary_30d;
CREATE TABLE dws_page_type_summary_30d (
                                           page_type STRING COMMENT '页面类型',
                                           device_type STRING COMMENT '设备类型',
                                           visit_count BIGINT COMMENT '访问次数',
                                           visitor_count BIGINT COMMENT '访客数',
                                           avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                           exit_count BIGINT COMMENT '退出次数',
                                           bounce_rate DOUBLE COMMENT '跳出率',
                                           conversion_count BIGINT COMMENT '转化次数'
)
    COMMENT '页面类型汇总表(30天)'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dws/dws_page_type_summary_30d/';

INSERT OVERWRITE TABLE dws_page_type_summary_30d PARTITION(dt='2025-07-31')
SELECT
    pvd.page_type,
    pvd.device_type,
    COUNT(*) AS visit_count,
    COUNT(DISTINCT pvd.user_id) AS visitor_count,
    ROUND(AVG(pvd.stay_duration), 2) AS avg_stay_duration,
    SUM(CASE WHEN pvd.is_last_page THEN 1 ELSE 0 END) AS exit_count,
    ROUND(
                    COUNT(DISTINCT CASE WHEN sp.page_count = 1 THEN sp.session_id END) * 1.0
                / COUNT(DISTINCT sp.session_id),
                    4
        ) AS bounce_rate,
    COUNT(DISTINCT CASE WHEN uc.is_converted = 1 THEN uc.session_id END) AS conversion_count
FROM (
         SELECT
             *,
             ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time DESC) = 1 AS is_last_page
         FROM dwd_page_visit_detail
         WHERE dt BETWEEN DATE_SUB('2025-07-31', 29) AND '2025-07-31'  -- 动态计算30天范围[10](@ref)
     ) pvd
         JOIN (
    -- 计算每个会话的总页面数（用于跳出率判断）
    SELECT
        session_id,
        COUNT(*) AS page_count
    FROM dwd_page_visit_detail
    WHERE dt BETWEEN DATE_SUB('2025-07-31', 29) AND '2025-07-31'
    GROUP BY session_id
) sp ON pvd.session_id = sp.session_id
         LEFT JOIN dwd_user_conversion uc  -- 关联用户转化表
                   ON pvd.session_id = uc.session_id
                       AND uc.dt BETWEEN DATE_SUB('2025-07-31', 29) AND '2025-07-31'
GROUP BY
    pvd.page_type,
    pvd.device_type;

SELECT * FROM dws_page_type_summary_30d;