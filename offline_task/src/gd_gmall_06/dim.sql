SET hive.exec.mode.local.auto=true;
SET hive.mapred.local.mem=4096;
SET hive.auto.convert.join=true;
set mapreduce.map.memory.mb=10150;
set mapreduce.map.java.opts=-Xmx6144m;
set mapreduce.reduce.memory.mb=10150;
set mapreduce.reduce.java.opts=-Xmx8120m;

use gmall_06;

-- 1. 商品维度拉链表 (dim_sku_info_zip)
DROP TABLE IF EXISTS dim_sku_info_zip;
CREATE TABLE IF NOT EXISTS dim_sku_info_zip (
                                                sku_id STRING COMMENT '商品货号',
                                                sku_title STRING COMMENT '商品标题',
                                                sku_main_image STRING COMMENT '主图URL',
                                                category_id STRING COMMENT '类目ID',
                                                category_name STRING COMMENT '类目名称',
                                                brand_id STRING COMMENT '品牌ID',
                                                brand_name STRING COMMENT '品牌名称',
                                                product_model STRING COMMENT '商品型号',
                                                sale_price DECIMAL(10,2) COMMENT '销售价',
    weight DECIMAL(10,2) COMMENT '重量(kg)',
    color STRING COMMENT '颜色',
    size STRING COMMENT '尺寸',
    store_id STRING COMMENT '店铺ID',
    putaway_time TIMESTAMP COMMENT '上架时间',
    is_online TINYINT COMMENT '是否上架',
    start_date STRING COMMENT '生效日期',
    end_date STRING COMMENT '失效日期'
    )
    COMMENT '商品维度拉链表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dim/dim_sku_info_zip'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE dim_sku_info_zip
SELECT
    sku_id,
    COALESCE(sku_title, '未知') AS sku_title,
    COALESCE(sku_main_image, '') AS sku_main_image,
    category_id,
    COALESCE(category_name, '其他') AS category_name,
    brand_id,
    COALESCE(brand_name, '其他') AS brand_name,
    COALESCE(product_model, '') AS product_model,
    COALESCE(CAST(sale_price AS DECIMAL(10,2)), 0) AS sale_price,
    COALESCE(CAST(weight AS DECIMAL(10,2)), 0) AS weight,
    COALESCE(color, '') AS color,
    COALESCE(size, '') AS size,
    store_id,
    putaway_time,
    is_online,
    '2025-08-08' AS start_date,
    '9999-12-31' AS end_date
FROM ods_sku_base_info
WHERE dt = '2025-08-08'
  AND is_delete = 0
  AND sku_id IS NOT NULL;


select  * from dim_sku_info_zip;

-- 2. 商品上新标签拉链表 (dim_sku_new_arrival_tag_zip)
drop table IF EXISTS dim_sku_new_arrival_tag_zip;
DROP TABLE IF EXISTS dim_sku_new_arrival_tag_zip;
CREATE TABLE IF NOT EXISTS dim_sku_new_arrival_tag_zip (
                                                           id STRING COMMENT '记录ID',
                                                           sku_id STRING COMMENT '商品货号',
                                                           new_type STRING COMMENT '新品类型',
                                                           is_tmall_new TINYINT COMMENT '是否天猫新品',
                                                           new_tag_valid_days INT COMMENT '标签有效期(天)',
                                                           new_tag_start_time TIMESTAMP COMMENT '标签生效时间',
                                                           new_tag_end_time TIMESTAMP COMMENT '标签失效时间',
                                                           audit_status STRING COMMENT '审核状态',
                                                           start_date STRING COMMENT '生效日期',
                                                           end_date STRING COMMENT '失效日期'
)
    COMMENT '商品上新标签拉链表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dim/dim_sku_new_arrival_tag_zip'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dim_sku_new_arrival_tag_zip
SELECT
    id,
    sku_id,
    COALESCE(new_type, '普通新品') AS new_type,
    COALESCE(is_tmall_new, 0) AS is_tmall_new,
    COALESCE(new_tag_valid_days, 30) AS new_tag_valid_days,
    new_tag_start_time,
    new_tag_end_time,
    COALESCE(new_tag_audit_status, '待审') AS new_tag_audit_status,
    '2025-08-08' AS start_date,
    '9999-12-31' AS end_date
FROM ods_sku_new_arrival_tag
WHERE dt = '2025-08-08'
  AND new_tag_end_time >= '2025-08-08'
  AND sku_id IS NOT NULL;

select * from dim_sku_new_arrival_tag_zip;

-- 3. 商品支付维度拉链表 (dim_sku_payment_detail_zip)
drop table  IF EXISTS dim_sku_payment_detail_zip;
DROP TABLE IF EXISTS dim_sku_payment_detail_zip;
CREATE TABLE IF NOT EXISTS dim_sku_payment_detail_zip (
                                                          payment_id STRING COMMENT '支付单ID',
                                                          sku_id STRING COMMENT '商品货号',
                                                          user_id STRING COMMENT '用户ID',
                                                          payment_amount DECIMAL(16,2) COMMENT '支付金额',
    payment_quantity INT COMMENT '支付数量',
    payment_time TIMESTAMP COMMENT '支付时间',
    payment_type STRING COMMENT '支付方式',
    refund_status TINYINT COMMENT '退款状态',
    store_id STRING COMMENT '店铺ID',
    start_date STRING COMMENT '生效日期',
    end_date STRING COMMENT '失效日期'
    )
    COMMENT '商品支付维度拉链表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dim/dim_sku_payment_detail_zip'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dim_sku_payment_detail_zip
SELECT
    payment_id,
    sku_id,
    user_id,
    CAST(COALESCE(payment_amount, 0) AS DECIMAL(16,2)) AS payment_amount,
    COALESCE(payment_quantity, 0) AS payment_quantity,
    payment_time,
    COALESCE(payment_type, '未知') AS payment_type,
    COALESCE(refund_status, 0) AS refund_status,
    store_id,
    '2025-08-08' AS start_date,
    '9999-12-31' AS end_date
FROM ods_sku_payment_detail
WHERE dt = '2025-08-08'
  AND payment_status = 1
  AND payment_time IS NOT NULL
  AND sku_id IS NOT NULL; -- 过滤空 sku_id

select * from dim_sku_payment_detail_zip;

-- 4. 商品年度上新维度拉链表 (dim_sku_yearly_new_record_zip)
drop table IF EXISTS dim_sku_yearly_new_record_zip;
DROP TABLE IF EXISTS dim_sku_yearly_new_record_zip;
CREATE TABLE IF NOT EXISTS dim_sku_yearly_new_record_zip (
                                                             id STRING COMMENT '记录ID',
                                                             sku_id STRING COMMENT '商品货号',
                                                             putaway_date DATE COMMENT '上架日期',
                                                             putaway_year INT COMMENT '上架年份',
                                                             putaway_month INT COMMENT '上架月份',
                                                             new_type STRING COMMENT '新品类型',
                                                             initial_sale_price DECIMAL(10,2) COMMENT '初始售价',
    category_id STRING COMMENT '类目ID',
    brand_id STRING COMMENT '品牌ID',
    start_date STRING COMMENT '生效日期',
    end_date STRING COMMENT '失效日期'
    )
    COMMENT '商品年度上新维度拉链表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dim/dim_sku_yearly_new_record_zip'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dim_sku_yearly_new_record_zip
SELECT
    id,
    sku_id,
    putaway_date,
    putaway_year,
    putaway_month,
    COALESCE(new_type, '普通新品') AS new_type,
    CAST(COALESCE(initial_sale_price, 0) AS DECIMAL(10,2)) AS initial_sale_price,
    category_id,
    brand_id,
    '2025-08-08' AS start_date,
    '9999-12-31' AS end_date
FROM ods_sku_yearly_new_record
WHERE dt = '2025-08-08'
  AND putaway_date IS NOT NULL
  AND putaway_year = 2025
  AND sku_id IS NOT NULL; -- 过滤空 sku_id

select * from dim_sku_yearly_new_record_zip;

-- 5. 店铺维度拉链表 (dim_store_info_zip)
drop table IF EXISTS dim_store_info_zip;
DROP TABLE IF EXISTS dim_store_info_zip;
CREATE TABLE IF NOT EXISTS dim_store_info_zip (
                                                  store_id STRING COMMENT '店铺ID',
                                                  store_name STRING COMMENT '店铺名称',
                                                  store_type STRING COMMENT '店铺类型',
                                                  platform STRING COMMENT '所属平台',
                                                  category_main STRING COMMENT '主营类目',
                                                  open_date DATE COMMENT '开店日期',
                                                  level TINYINT COMMENT '店铺等级',
                                                  start_date STRING COMMENT '生效日期',
                                                  end_date STRING COMMENT '失效日期'
)
    COMMENT '店铺维度拉链表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dim/dim_store_info_zip'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dim_store_info_zip
SELECT
    store_id,
    COALESCE(store_name, '未知店铺') AS store_name,
    COALESCE(store_type, '未知类型') AS store_type,
    COALESCE(platform, '未知平台') AS platform,
    COALESCE(category_main, '未分类') AS category_main,
    open_date,
    COALESCE(level, 0) AS level,
    '2025-08-08' AS start_date,
    '9999-12-31' AS end_date
FROM ods_store_base_info
WHERE dt = '2025-08-08'
  AND open_date IS NOT NULL
  AND store_id IS NOT NULL; -- 过滤空 store_id

select * from dim_store_info_zip;
