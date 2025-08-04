set hive.exec.mode.local.auto=True;
use gmall_10;

--1. 页面维度拉链表 (dim_page_info_zip)
-- 创建页面维度拉链表
CREATE TABLE IF NOT EXISTS dim_page_info_zip (
                                                 page_id STRING COMMENT '页面唯一标识',
                                                 page_name STRING COMMENT '页面名称',
                                                 page_type STRING COMMENT '页面类型',
                                                 data_source STRING COMMENT '数据来源',
                                                 start_date STRING COMMENT '记录生效日期',
                                                 end_date STRING COMMENT '记录失效日期'
) COMMENT '页面信息维度拉链表'
    PARTITIONED BY (dt STRING COMMENT '快照日期')
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dim/dim_page_info_zip'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并初始化页面维度 (dt=2025-08-05)
-- 修正表名（去除重复的dim_前缀）并调整结构，新增is_valid字段标识有效性
INSERT OVERWRITE TABLE dim_page_info_zip  -- 表名修正：dim_dim_page_info_zip → dim_page_info_zip
SELECT
    page_id,
    page_name,
    page_type,
    data_source,
    '2025-08-05' AS start_date,
    '9999-12-31' AS end_date,
    1 AS is_valid  -- 新增：1表示有效记录
FROM (
         SELECT
             page_id,
             -- 使用LAST_VALUE获取最新页面名称（按访问时间排序）
             LAST_VALUE(page_name) OVER(
            PARTITION BY page_id
            ORDER BY visit_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS page_name,
        -- 使用LAST_VALUE获取最新页面类型
                 LAST_VALUE(page_type) OVER(
            PARTITION BY page_id
            ORDER BY visit_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS page_type,
                 'business_analyzer' AS data_source,
             -- 按页面ID分区，取最新访问时间的记录（去重）
             ROW_NUMBER() OVER(PARTITION BY page_id ORDER BY visit_time DESC) AS rn
         FROM ods_page_visit_base
         WHERE dt = '2025-08-05'
           AND page_id IS NOT NULL

     ) t
WHERE rn = 1;
SELECT * FROM dim_page_info_zip;
--2. 页面板块维度拉链表 (dim_page_block_zip)
-- 创建板块维度拉链表
CREATE TABLE IF NOT EXISTS dim_page_block_zip (
                                                  block_id STRING COMMENT '板块唯一标识',
                                                  block_name STRING COMMENT '板块名称',
                                                  page_id STRING COMMENT '所属页面ID',
                                                  start_date STRING COMMENT '记录生效日期',
                                                  end_date STRING COMMENT '记录失效日期'
) COMMENT '页面板块信息维度拉链表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dim/dim_page_block_zip'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并初始化板块维度 (dt=2025-08-05)
INSERT OVERWRITE TABLE dim_page_block_zip PARTITION(dt='2025-08-05')
SELECT
    block_id,
    block_name,
    page_id,
    '2025-08-05' AS start_date,
    '9999-12-31' AS end_date
FROM (
         SELECT
             block_id,
             LAST_VALUE(block_name) OVER(
            PARTITION BY block_id
            ORDER BY collect_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS block_name,
                 LAST_VALUE(page_id) OVER(
            PARTITION BY block_id
            ORDER BY collect_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS page_id,
                 ROW_NUMBER() OVER(PARTITION BY block_id ORDER BY collect_time DESC) AS rn
         FROM ods_page_block_click
         WHERE dt = '2025-08-05'
           AND block_id IS NOT NULL
           AND LENGTH(block_id) > 0 -- 过滤空ID
     ) t
WHERE rn = 1;

select  * from dim_page_block_zip;
--3. 页面模块维度拉链表 (dim_page_module_zip)
-- 创建模块维度拉链表
CREATE TABLE IF NOT EXISTS dim_page_module_zip (
                                                   module_id STRING COMMENT '模块唯一标识',
                                                   module_name STRING COMMENT '模块名称',
                                                   module_type STRING COMMENT '模块类型',
                                                   page_id STRING COMMENT '所属页面ID',
                                                   start_date STRING COMMENT '记录生效日期',
                                                   end_date STRING COMMENT '记录失效日期'
) COMMENT '页面模块信息维度拉链表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dim/dim_page_module_zip'
    TBLPROPERTIES ("orc.compress"="snappy");

-- 清洗并初始化模块维度 (dt=2025-08-05)
INSERT OVERWRITE TABLE dim_page_module_zip PARTITION(dt='2025-08-05')
SELECT
    module_id,
    module_name,
    module_type,
    page_id,
    '2025-08-05' AS start_date,
    '9999-12-31' AS end_date
FROM (
         SELECT
             module_id,
             LAST_VALUE(module_name) OVER(
            PARTITION BY module_id
            ORDER BY stat_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS module_name,
                 LAST_VALUE(module_type) OVER(
            PARTITION BY module_id
            ORDER BY stat_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS module_type,
                 LAST_VALUE(page_id) OVER(
            PARTITION BY module_id
            ORDER BY stat_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS page_id,
                 ROW_NUMBER() OVER(PARTITION BY module_id ORDER BY stat_time DESC) AS rn
         FROM ods_page_module_detail
         WHERE dt = '2025-08-05'
           AND module_id IS NOT NULL
           AND expose_pv > 0 -- 过滤无曝光的无效模块
     ) t
WHERE rn = 1;

select  * from dim_page_module_zip;
--4. 商品维度拉链表 (dim_product_info_zip)
-- 创建商品维度拉链表
CREATE TABLE IF NOT EXISTS dim_product_info_zip (
                                                    product_id STRING COMMENT '商品ID',
                                                    product_name STRING COMMENT '商品名称',
                                                    start_date STRING COMMENT '记录生效日期',
                                                    end_date STRING COMMENT '记录失效日期'
) COMMENT '商品信息维度拉链表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/dim/dim_product_info_zip'
    TBLPROPERTIES ("orc.compress"="snappy");


-- 清洗并初始化商品维度 (dt=2025-08-05)
-- 注：实际商品名称需从其他系统获取，此处使用占位符
INSERT OVERWRITE TABLE dim_product_info_zip PARTITION(dt='2025-08-05')
SELECT
    product_id,
    CONCAT('商品_', product_id) AS product_name,  -- 实际场景应从商品系统获取
    '2025-08-05' AS start_date,
    '9999-12-31' AS end_date
FROM (
         SELECT
             product_id,
             ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY guide_time DESC) AS rn
         FROM ods_page_guide_product
         WHERE dt = '2025-08-05'
           AND product_id IS NOT NULL
         -- 假设商品ID为19位
     ) t
WHERE rn = 1;
select  * from dim_product_info_zip;