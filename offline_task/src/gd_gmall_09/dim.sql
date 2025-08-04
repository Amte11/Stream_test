set hive.exec.mode.local.auto=True;
use ds_path;

--1. 用户维度拉链表（dim_user_info）
--建表语句（dt 分区固定为 2025-07-31）
DROP TABLE IF EXISTS dim_user_info;
CREATE TABLE dim_user_info (
                               user_id BIGINT COMMENT '用户ID',
                               user_name STRING COMMENT '用户名',
                               gender STRING COMMENT '性别',
                               age INT COMMENT '年龄',
                               register_time TIMESTAMP COMMENT '注册时间',
                               vip_level INT COMMENT 'VIP等级',
                               province STRING COMMENT '省份',
                               city STRING COMMENT '城市',
                               phone_prefix STRING COMMENT '手机号前3位',
                               start_date STRING COMMENT '有效开始日期',
                               end_date STRING COMMENT '有效结束日期',
                               is_current TINYINT COMMENT '是否当前有效(1是/0否)'
) COMMENT '用户维度拉链表'
PARTITIONED BY (dt STRING)  -- 分区字段
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dim/dim_user_info/';
--拉链更新 SQL（dt=2025-07-31）
INSERT OVERWRITE TABLE dim_user_info PARTITION (dt = '2025-07-31')
-- 新增或变更的当前有效记录
SELECT
    user_id,
    user_name,
    gender,
    age,
    register_time,
    vip_level,
    province,
    city,
    phone_prefix,
    '2025-07-31' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_current
FROM ods_user_info
WHERE dt = '2025-07-31'  -- 仅处理当前日期的ODS数据
UNION ALL
-- 历史有效记录中发生变更的旧记录（标记失效）
SELECT
    a.user_id,
    a.user_name,
    a.gender,
    a.age,
    a.register_time,
    a.vip_level,
    a.province,
    a.city,
    a.phone_prefix,
    a.start_date,
    '2025-07-30' AS end_date,  -- 失效日为当前日期前一天
    0 AS is_current
FROM dim_user_info a
         JOIN ods_user_info b
              ON a.user_id = b.user_id AND b.dt = '2025-07-31'
WHERE a.dt = '2025-07-31' AND a.is_current = 1  -- 关联当前分区的历史有效记录
  AND (a.gender != b.gender OR a.age != b.age OR a.vip_level != b.vip_level);

select * from dim_user_info;
--2. 商品维度拉链表（dim_product_info）
--建表语句（dt 分区固定为 2025-07-31）
DROP TABLE IF EXISTS dim_product_info;
CREATE TABLE dim_product_info (
                                  product_id BIGINT COMMENT '商品ID',
                                  product_name STRING COMMENT '商品名称',
                                  category_id BIGINT COMMENT '类目ID',
                                  category_name STRING COMMENT '类目名称',
                                  brand_id BIGINT COMMENT '品牌ID',
                                  brand_name STRING COMMENT '品牌名称',
                                  price DECIMAL(18,2) COMMENT '商品价格',
                                  cost_price DECIMAL(18,2) COMMENT '成本价',
                                  status TINYINT COMMENT '状态(1上架/0下架)',
                                  create_time TIMESTAMP COMMENT '创建时间',
                                  start_date STRING COMMENT '有效开始日期',
                                  end_date STRING COMMENT '有效结束日期',
                                  is_current TINYINT COMMENT '是否当前有效(1是/0否)'
) COMMENT '商品维度拉链表'
PARTITIONED BY (dt STRING)  -- 分区字段
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dim/dim_product_info/';


--拉链更新 SQL（dt=2025-07-31）
INSERT OVERWRITE TABLE dim_product_info PARTITION (dt = '2025-07-31')
-- 新增或变更的当前有效记录
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    brand_id,
    brand_name,
    price,
    cost_price,
    status,
    create_time,
    '2025-07-31' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_current
FROM ods_product_info
WHERE dt = '2025-07-31'  -- 仅处理当前日期的ODS数据
UNION ALL
-- 历史有效记录中发生变更的旧记录（标记失效）
SELECT
    a.product_id,
    a.product_name,
    a.category_id,
    a.category_name,
    a.brand_id,
    a.brand_name,
    a.price,
    a.cost_price,
    a.status,
    a.create_time,
    a.start_date,
    '2025-07-30' AS end_date,
    0 AS is_current
FROM dim_product_info a
         JOIN ods_product_info b
              ON a.product_id = b.product_id AND b.dt = '2025-07-31'
WHERE a.dt = '2025-07-31' AND a.is_current = 1
  AND (a.price != b.price OR a.status != b.status);


select * from dim_product_info;
--3. 店铺页面维度拉链表（dim_shop_page_info）
--建表语句（dt 分区固定为 2025-07-31）

DROP TABLE IF EXISTS dim_shop_page_info;
CREATE TABLE dim_shop_page_info (
                                    page_id BIGINT COMMENT '页面ID',
                                    page_name STRING COMMENT '页面名称',
                                    page_url STRING COMMENT '页面URL',
                                    page_type STRING COMMENT '页面类型(home/activity/category/new等)',
                                    page_level TINYINT COMMENT '页面层级',
                                    parent_page_id BIGINT COMMENT '父页面ID',
                                    is_active TINYINT COMMENT '是否有效(1是/0否)',
                                    create_time TIMESTAMP COMMENT '创建时间',
                                    start_date STRING COMMENT '有效开始日期',
                                    end_date STRING COMMENT '有效结束日期',
                                    is_current TINYINT COMMENT '是否当前有效(1是/0否)'
) COMMENT '店铺页面维度拉链表'
PARTITIONED BY (dt STRING)  -- 分区字段
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dim/dim_shop_page_info/';

--拉链更新 SQL（dt=2025-07-31）
INSERT OVERWRITE TABLE dim_shop_page_info PARTITION (dt = '2025-07-31')
-- 新增或变更的当前有效记录
SELECT
    page_id,
    page_name,
    page_url,
    page_type,
    page_level,
    parent_page_id,
    is_active,
    create_time,
    '2025-07-31' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_current
FROM ods_shop_page_info
WHERE dt = '2025-07-31'  -- 仅处理当前日期的ODS数据
UNION ALL
-- 历史有效记录中发生变更的旧记录（标记失效）
SELECT
    a.page_id,
    a.page_name,
    a.page_url,
    a.page_type,
    a.page_level,
    a.parent_page_id,
    a.is_active,
    a.create_time,
    a.start_date,
    '2025-07-30' AS end_date,
    0 AS is_current
FROM dim_shop_page_info a
         JOIN ods_shop_page_info b
              ON a.page_id = b.page_id AND b.dt = '2025-07-31'
WHERE a.dt = '2025-07-31' AND a.is_current = 1
  AND (a.page_type != b.page_type OR a.is_active != b.is_active);  -- 🔶1-29🔶


select * from dim_shop_page_info;
--4. 流量来源维度拉链表（dim_traffic_source）
--建表语句（dt 分区固定为 2025-07-31）
DROP TABLE IF EXISTS dim_traffic_source;
CREATE TABLE dim_traffic_source (
                                    source_id BIGINT COMMENT '来源ID',
                                    source_type STRING COMMENT '来源类型(search/social/ad等)',
                                    source_name STRING COMMENT '来源名称',
                                    source_url STRING COMMENT '来源URL',
                                    campaign_id BIGINT COMMENT '活动ID',
                                    campaign_name STRING COMMENT '活动名称',
                                    start_date STRING COMMENT '有效开始日期',
                                    end_date STRING COMMENT '有效结束日期',
                                    is_current TINYINT COMMENT '是否当前有效(1是/0否)'
) COMMENT '流量来源维度拉链表'
PARTITIONED BY (dt STRING)  -- 分区字段
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dim/dim_traffic_source/';

--拉链更新 SQL（dt=2025-07-31）
INSERT OVERWRITE TABLE dim_traffic_source PARTITION (dt = '2025-07-31')
-- 新增或变更的当前有效记录
SELECT
    source_id,
    source_type,
    source_name,
    source_url,
    campaign_id,
    campaign_name,
    '2025-07-31' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_current
FROM ods_traffic_source
WHERE dt = '2025-07-31'  -- 仅处理当前日期的ODS数据
UNION ALL
-- 历史有效记录中发生变更的旧记录（标记失效）
SELECT
    a.source_id,
    a.source_type,
    a.source_name,
    a.source_url,
    a.campaign_id,
    a.campaign_name,
    a.start_date,
    '2025-07-30' AS end_date,
    0 AS is_current
FROM dim_traffic_source a
         JOIN ods_traffic_source b
              ON a.source_id = b.source_id AND b.dt = '2025-07-31'
WHERE a.dt = '2025-07-31' AND a.is_current = 1
  AND a.source_type != b.source_type;

select * from dim_traffic_source;

--5. 营销活动维度拉链表（dim_marketing_activity）
--建表语句（dt 分区固定为 2025-07-31）
DROP TABLE IF EXISTS dim_marketing_activity;
CREATE TABLE dim_marketing_activity (
                                        activity_id BIGINT COMMENT '活动ID',
                                        activity_name STRING COMMENT '活动名称',
                                        start_time TIMESTAMP COMMENT '开始时间',
                                        end_time TIMESTAMP COMMENT '结束时间',
                                        activity_type STRING COMMENT '活动类型',
                                        start_date STRING COMMENT '有效开始日期',
                                        end_date STRING COMMENT '有效结束日期',
                                        is_current TINYINT COMMENT '是否当前有效(1是/0否)'
) COMMENT '营销活动维度拉链表'
PARTITIONED BY (dt STRING)  -- 分区字段
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dim/dim_marketing_activity/';

--拉链更新 SQL（dt=2025-07-31）
INSERT OVERWRITE TABLE dim_marketing_activity PARTITION (dt = '2025-07-31')
-- 新增或变更的当前有效记录
SELECT
    activity_id,
    activity_name,
    start_time,
    end_time,
    activity_type,
    '2025-07-31' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_current
FROM ods_marketing_activity
WHERE dt = '2025-07-31'  -- 仅处理当前日期的ODS数据
UNION ALL
-- 历史有效记录中发生变更的旧记录（标记失效）
SELECT
    a.activity_id,
    a.activity_name,
    a.start_time,
    a.end_time,
    a.activity_type,
    a.start_date,
    '2025-07-30' AS end_date,
    0 AS is_current
FROM dim_marketing_activity a
         JOIN ods_marketing_activity b
              ON a.activity_id = b.activity_id AND b.dt = '2025-07-31'
WHERE a.dt = '2025-07-31' AND a.is_current = 1
  AND (a.start_time != b.start_time OR a.end_time != b.end_time);



select * from dim_marketing_activity;
--6. 页面关系维度拉链表（dim_page_relationship）
--建表语句（dt 分区固定为 2025-07-31）
DROP TABLE IF EXISTS dim_page_relationship;
CREATE TABLE dim_page_relationship (
                                       from_page_id BIGINT COMMENT '来源页面ID',
                                       to_page_id BIGINT COMMENT '去向页面ID',
                                       relation_type STRING COMMENT '关系类型',
                                       create_time TIMESTAMP COMMENT '创建时间',
                                       start_date STRING COMMENT '有效开始日期',
                                       end_date STRING COMMENT '有效结束日期',
                                       is_current TINYINT COMMENT '是否当前有效(1是/0否)'
) COMMENT '页面关系维度拉链表'
PARTITIONED BY (dt STRING)  -- 分区字段
STORED AS ORC
LOCATION '/warehouse/gd_gmall/dim/dim_page_relationship/';

--拉链更新 SQL（dt=2025-07-31）
INSERT OVERWRITE TABLE dim_page_relationship PARTITION (dt = '2025-07-31')
-- 新增或变更的当前有效记录
SELECT
    from_page_id,
    to_page_id,
    relation_type,
    create_time,
    '2025-07-31' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_current
FROM ods_page_relationship
WHERE dt = '2025-07-31'  -- 仅处理当前日期的ODS数据
UNION ALL
-- 历史有效记录中发生变更的旧记录（标记失效）
SELECT
    a.from_page_id,
    a.to_page_id,
    a.relation_type,
    a.create_time,
    a.start_date,
    '2025-07-31' AS end_date,
    0 AS is_current
FROM dim_page_relationship a
         JOIN ods_page_relationship b
              ON a.from_page_id = b.from_page_id AND a.to_page_id = b.to_page_id AND b.dt = '2025-07-31'
WHERE a.dt = '2025-07-31' AND a.is_current = 1
  AND a.relation_type != b.relation_type;

select * from dim_page_relationship;