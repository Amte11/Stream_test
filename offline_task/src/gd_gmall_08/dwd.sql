SET hive.exec.mode.local.auto=true;
USE gmall_08;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;


--1. 优惠活动事实表
CREATE EXTERNAL TABLE dwd_special_offer_activity (
    `activity_id` BIGINT COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `activity_level` STRING COMMENT '活动级别',
    `offer_type` STRING COMMENT '优惠类型',
    `start_date` DATE COMMENT '开始日期',
    `end_date` DATE COMMENT '结束日期',
    `max_custom_amount` DECIMAL(10,2) COMMENT '最大优惠金额',
    `shop_id` BIGINT COMMENT '店铺ID'
) COMMENT '优惠活动事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dwd/dwd_special_offer_activity/';

INSERT OVERWRITE TABLE dwd_special_offer_activity PARTITION(dt='2025-08-04')
SELECT
    activity_id,
    activity_name,
    CASE activity_level
        WHEN 1 THEN '商品级'
        WHEN 2 THEN 'SKU级'
        ELSE '普通级'
        END AS activity_level,
    CASE offer_type
        WHEN 1 THEN '固定优惠券'
        WHEN 2 THEN '折扣券'
        WHEN 3 THEN '满减券'
        ELSE '其他'
        END AS offer_type,
    TO_DATE(start_time) AS start_date,
    TO_DATE(end_time) AS end_date,
    max_custom_amount,
    shop_id
FROM ods_special_offer_activity
WHERE dt = '2025-08-04'
  AND status = 1 -- 有效活动
  AND TO_DATE(end_time) >= '2025-08-04';

select  * FROM dwd_special_offer_activity;


--2. 优惠商品事实表
CREATE EXTERNAL TABLE dwd_offer_products (
    `product_id` BIGINT COMMENT '商品ID',
    `sku_id` BIGINT COMMENT 'SKU ID',
    `activity_id` BIGINT COMMENT '活动ID',
    `fixed_offer_amount` DECIMAL(10,2) COMMENT '固定优惠',
    `max_offer_amount` DECIMAL(10,2) COMMENT '最大优惠',
    `purchase_limit` INT COMMENT '购买限制'
) COMMENT '优惠商品事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dwd/dwd_offer_products/';

INSERT OVERWRITE TABLE dwd_offer_products PARTITION(dt='2025-08-04')
SELECT
    p.product_id,
    s.sku_id,
    p.activity_id,
    COALESCE(s.fixed_offer_amount, p.fixed_offer_amount) AS fixed_offer_amount, -- SKU级优先
    COALESCE(s.max_offer_amount, p.max_offer_amount) AS max_offer_amount,
    p.purchase_limit
FROM ods_special_offer_products p
         LEFT JOIN ods_special_offer_skus s
                   ON p.product_id = s.product_id
                       AND s.dt = '2025-08-04'
WHERE p.dt = '2025-08-04'
  AND p.status = 1;
select  * FROM dwd_offer_products;

--3. 优惠发送事实表
CREATE EXTERNAL TABLE dwd_offer_send_records (
    `record_id` BIGINT COMMENT '记录ID',
    `activity_id` BIGINT COMMENT '活动ID',
    `customer_id` BIGINT COMMENT '客户ID',
    `cs_id` BIGINT COMMENT '客服ID',
    `offer_type` STRING COMMENT '优惠类型',
    `send_time` TIMESTAMP COMMENT '发送时间',
    `expire_time` TIMESTAMP COMMENT '过期时间',
    `valid_hours` INT COMMENT '有效期(小时)'
) COMMENT '优惠发送事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dwd/dwd_offer_send_records/';

INSERT OVERWRITE TABLE dwd_offer_send_records PARTITION(dt='2025-08-04')
SELECT
    s.record_id,
    s.activity_id,
    s.customer_id,
    s.cs_id,
    CASE a.offer_type
        WHEN 1 THEN '固定优惠券'
        WHEN 2 THEN '折扣券'
        WHEN 3 THEN '满减券'
        ELSE '其他'
        END AS offer_type,
    CAST(s.send_time AS TIMESTAMP) AS send_time,
    CAST(s.expire_time AS TIMESTAMP) AS expire_time,
    s.valid_hours
FROM ods_offer_send_records s
         JOIN ods_special_offer_activity a
              ON s.activity_id = a.activity_id
                  AND a.dt = '2025-08-04'
WHERE s.dt = '2025-08-04'
  AND s.status = 1;

select  * FROM dwd_offer_send_records;

--4.优惠核销事实表
CREATE EXTERNAL TABLE dwd_offer_redemption_records (
    `redemption_id` BIGINT COMMENT '核销ID',
    `record_id` BIGINT COMMENT '记录ID',
    `order_id` BIGINT COMMENT '订单ID',
    `redemption_time` TIMESTAMP COMMENT '核销时间',
    `actual_offer_amount` DECIMAL(10,2) COMMENT '实际优惠金额',
    `payment_amount` DECIMAL(10,2) COMMENT '支付金额'
) COMMENT '优惠核销事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dwd/dwd_offer_redemption_records/';


-- 工单编号: 大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
INSERT OVERWRITE TABLE dwd_offer_redemption_records PARTITION(dt='2025-08-04')
SELECT
    redemption_id,
    record_id,
    order_id,
    -- 使用 CAST 替代 TO_TIMESTAMP [4,9](@ref)
    CAST(redemption_time AS TIMESTAMP) AS redemption_time,
    actual_offer_amount,
    payment_amount
FROM ods_offer_redemption_records
WHERE dt = '2025-08-04'
  AND redemption_time IS NOT NULL
  AND actual_offer_amount > 0;

---5. 客服服务事实表
-- 建表语句
CREATE EXTERNAL TABLE dwd_cs_service_records (
    `cs_id` BIGINT COMMENT '客服ID',
    `cs_name` STRING COMMENT '客服姓名',
    `department` STRING COMMENT '部门',
    `send_count` BIGINT COMMENT '发送优惠次数',
    `redemption_count` BIGINT COMMENT '核销次数',
    `avg_offer_amount` DECIMAL(10,2) COMMENT '平均优惠金额'
) COMMENT '客服服务事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dwd/dwd_cs_service_records/';


INSERT OVERWRITE TABLE dwd_cs_service_records PARTITION(dt='2025-08-04')
SELECT
    c.cs_id,
    c.cs_name,
    COALESCE(c.department, '未知部门') AS department,
    COUNT(DISTINCT s.record_id) AS send_count,
    COUNT(DISTINCT r.redemption_id) AS redemption_count,
    ROUND(AVG(s.offer_amount), 2) AS avg_offer_amount
FROM ods_customer_service_info c
         LEFT JOIN ods_offer_send_records s
                   ON c.cs_id = s.cs_id AND s.dt = '2025-08-04'
         LEFT JOIN ods_offer_redemption_records r
                   ON s.record_id = r.record_id AND r.dt = '2025-08-04'
WHERE c.dt = '2025-08-04'
  AND c.status = 1 -- 在职客服
GROUP BY c.cs_id, c.cs_name, c.department;

select  * FROM dwd_cs_service_records;

--6.客户优惠事实表
CREATE EXTERNAL TABLE dwd_customer_offer_summary (
    `customer_id` BIGINT COMMENT '客户ID',
    `offer_count` BIGINT COMMENT '获得优惠次数',
    `redemption_rate` DECIMAL(5,2) COMMENT '核销率',
    `total_savings` DECIMAL(15,2) COMMENT '总节省金额'
) COMMENT '客户优惠事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dwd/dwd_customer_offer_summary/';


INSERT OVERWRITE TABLE dwd_customer_offer_summary PARTITION(dt='2025-08-04')
SELECT
    s.customer_id,
    COUNT(DISTINCT s.record_id) AS offer_count,
    ROUND(COUNT(DISTINCT r.redemption_id) * 1.0 / COUNT(DISTINCT s.record_id), 2) AS redemption_rate,
    SUM(COALESCE(r.actual_offer_amount, 0)) AS total_savings
FROM ods_offer_send_records s
         LEFT JOIN ods_offer_redemption_records r
                   ON s.record_id = r.record_id AND r.dt = '2025-08-04'
WHERE s.dt = '2025-08-04'
GROUP BY s.customer_id;

select  * FROM dwd_customer_offer_summary;

