SET hive.exec.mode.local.auto=true;
CREATE DATABASE IF NOT EXISTS ds_path;
USE ds_path;

-- 1. 页面访问明细表 (核心表)
DROP TABLE IF EXISTS dwd_page_visit_detail;
CREATE TABLE dwd_page_visit_detail (
                                       log_id BIGINT COMMENT '日志ID',
                                       user_id BIGINT COMMENT '用户ID',
                                       session_id STRING COMMENT '会话ID',
                                       page_id BIGINT COMMENT '页面ID',
                                       page_name STRING COMMENT '页面名称',
                                       page_type STRING COMMENT '页面类型(shop_page/product_detail/other_page)',
                                       referer_page_id BIGINT COMMENT '来源页面ID',
                                       referer_page_name STRING COMMENT '来源页面名称',
                                       visit_time TIMESTAMP COMMENT '访问时间',
                                       stay_duration INT COMMENT '停留时长(秒)',
                                       device_type STRING COMMENT '设备类型(mobile/pc/applet)',
                                       province STRING COMMENT '省份',
                                       city STRING COMMENT '城市',
                                       is_new_visitor TINYINT COMMENT '是否新访客(1是/0否)',
                                       page_category STRING COMMENT '页面大类(shop_page/product_detail/other_page)'
)
    COMMENT '页面访问明细表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dwd/dwd_page_visit_detail/';

-- 数据清洗转换
INSERT OVERWRITE TABLE dwd_page_visit_detail PARTITION(dt='2025-07-31')
SELECT
    log.log_id,
    log.user_id,
    log.session_id,
    log.page_id,
    COALESCE(page.page_name, '未知页面') AS page_name,
    -- 按工单要求清洗页面类型
    CASE
        WHEN page.page_type IN ('home','activity','category','new','宝贝页') THEN 'shop_page'
        WHEN log.page_type = 'product' THEN 'product_detail'
        ELSE 'other_page'
        END AS page_type,
    log.referer_page_id,
    COALESCE(ref_page.page_name, '无来源') AS referer_page_name,
    log.visit_time,
    -- 清洗停留时长：负值设为0，超过1小时设为3600
    CASE
        WHEN log.stay_duration < 0 THEN 0
        WHEN log.stay_duration > 3600 THEN 3600
        ELSE log.stay_duration
        END AS stay_duration,
    -- 统一设备类型格式
    LOWER(log.device_type) AS device_type,
    COALESCE(log.province, '未知') AS province,
    COALESCE(log.city, '未知') AS city,
    log.is_new_visitor,
    -- 根据工单定义创建页面大类（避免重复计算，可重用page_type逻辑）
    CASE
        WHEN page.page_type IN ('home','activity','category','new','宝贝页') THEN 'shop_page'
        WHEN log.page_type = 'product' THEN 'product_detail'
        ELSE 'other_page'
        END AS page_category
FROM ods_page_visit_log log
         LEFT JOIN ods_shop_page_info page
                   ON log.page_id = page.page_id AND page.dt='2025-07-31'
         LEFT JOIN ods_shop_page_info ref_page
                   ON log.referer_page_id = ref_page.page_id AND ref_page.dt='2025-07-31'
WHERE log.dt='2025-07-31';

SELECT * FROM dwd_page_visit_detail;

-- 2. 会话路径表
DROP TABLE IF EXISTS dwd_session_path;
CREATE TABLE dwd_session_path (
                                  session_id STRING COMMENT '会话ID',
                                  user_id BIGINT COMMENT '用户ID',
                                  entry_page_id BIGINT COMMENT '入口页面ID',
                                  entry_time TIMESTAMP COMMENT '入口时间',
                                  exit_page_id BIGINT COMMENT '退出页面ID',
                                  exit_time TIMESTAMP COMMENT '退出时间',
                                  path_depth INT COMMENT '访问深度',
                                  total_duration INT COMMENT '总停留时长(秒)',
                                  device_type STRING COMMENT '设备类型',
                                  is_new_session TINYINT COMMENT '是否新会话'
)
    COMMENT '会话路径表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dwd/dwd_session_path/';

-- 会话路径处理
INSERT OVERWRITE TABLE dwd_session_path PARTITION(dt='2025-07-31')
SELECT
    session_id,
    user_id,
    entry_page_id,
    entry_time,
    exit_page_id,
    exit_time,
    path_depth,
    total_duration,
    device_type,
    is_new_visitor AS is_new_session
FROM (
         SELECT
             session_id,
             user_id,
             FIRST_VALUE(page_id) OVER w AS entry_page_id,
                 FIRST_VALUE(visit_time) OVER w AS entry_time,
                 LAST_VALUE(page_id) OVER w AS exit_page_id,
                 LAST_VALUE(visit_time) OVER w AS exit_time,
                 COUNT(page_id) OVER w AS path_depth,
                 SUM(stay_duration) OVER w AS total_duration,
                 FIRST_VALUE(device_type) OVER w AS device_type,
                 FIRST_VALUE(is_new_visitor) OVER w AS is_new_visitor,
                 ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_time) AS rn
         FROM dwd_page_visit_detail
         WHERE dt='2025-07-31'
             WINDOW w AS (PARTITION BY session_id ORDER BY visit_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
     ) t
WHERE rn = 1;

SELECT * FROM dwd_session_path;
-- 3. 页面跳转表
DROP TABLE IF EXISTS dwd_page_jump;
CREATE TABLE dwd_page_jump (
                               from_page_id BIGINT COMMENT '来源页面ID',
                               to_page_id BIGINT COMMENT '目标页面ID',
                               jump_count BIGINT COMMENT '跳转次数',
                               jump_users BIGINT COMMENT '跳转用户数'
)
    COMMENT '页面跳转表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dwd/dwd_page_jump/';

-- 页面跳转关系处理
INSERT OVERWRITE TABLE dwd_page_jump PARTITION(dt='2025-07-31')
SELECT
    current_page_id AS from_page_id,
    next_page_id AS to_page_id,
    COUNT(1) AS jump_count,
    COUNT(DISTINCT user_id) AS jump_users
FROM (
         SELECT
             log1.page_id AS current_page_id,
             log1.user_id,
             LEAD(log1.page_id) OVER (PARTITION BY log1.session_id ORDER BY log1.visit_time) AS next_page_id
         FROM dwd_page_visit_detail log1
         WHERE dt='2025-07-31'
     ) t
WHERE next_page_id IS NOT NULL
GROUP BY current_page_id, next_page_id;

SELECT * FROM dwd_page_jump;
-- 4. 用户转化表
DROP TABLE IF EXISTS dwd_user_conversion;
CREATE TABLE dwd_user_conversion (
                                     user_id BIGINT COMMENT '用户ID',
                                     session_id STRING COMMENT '会话ID',
                                     page_view_count INT COMMENT '页面浏览数',
                                     session_duration INT COMMENT '会话时长',
                                     is_converted TINYINT COMMENT '是否转化(1是/0否)',
                                     conversion_type STRING COMMENT '转化类型(view/order/payment)',
                                     conversion_value DECIMAL(18,2) COMMENT '转化价值'
)
    COMMENT '用户转化表'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dwd/dwd_user_conversion/';

-- 转化数据处理
INSERT OVERWRITE TABLE dwd_user_conversion PARTITION(dt='2025-07-31')
SELECT
    s.user_id,
    s.session_id,
    s.path_depth AS page_view_count,
    s.total_duration AS session_duration,
    CASE
        WHEN o.order_id IS NOT NULL AND (o.order_time BETWEEN s.entry_time AND s.exit_time) THEN 1
        WHEN o.order_id IS NOT NULL AND (o.payment_time BETWEEN s.entry_time AND s.exit_time) THEN 1
        ELSE 0
        END AS is_converted,
    CASE
        WHEN o.payment_time IS NOT NULL AND (o.payment_time BETWEEN s.entry_time AND s.exit_time) THEN 'payment'
        WHEN o.order_time IS NOT NULL AND (o.order_time BETWEEN s.entry_time AND s.exit_time) THEN 'order'
        ELSE 'view'
        END AS conversion_type,
    COALESCE(
            CASE
                WHEN o.payment_time IS NOT NULL AND (o.payment_time BETWEEN s.entry_time AND s.exit_time) THEN o.payment_amount
                ELSE 0
                END, 0
        ) AS conversion_value
FROM dwd_session_path s
         LEFT JOIN (
    SELECT
        user_id,
        order_id,
        order_time,
        payment_time,
        payment_amount
    FROM ods_order_info
    WHERE dt='2025-07-31'
) o ON s.user_id = o.user_id
    AND (
               (o.order_time BETWEEN s.entry_time AND s.exit_time)
               OR (o.payment_time BETWEEN s.entry_time AND s.exit_time)
           )
WHERE s.dt='2025-07-31';

SELECT * FROM dwd_user_conversion;