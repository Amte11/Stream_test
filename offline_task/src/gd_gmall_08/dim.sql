SET hive.exec.mode.local.auto=true;
USE gmall_08;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;


--一、客服维度拉链表
CREATE EXTERNAL TABLE dim_customer_service_zip (
    `cs_id` BIGINT COMMENT '客服ID',
    `cs_name` STRING COMMENT '客服姓名',
    `department` STRING COMMENT '部门',
    `position` STRING COMMENT '职位',
    `hire_date` DATE COMMENT '入职日期',
    `status` INT COMMENT '状态（0离职/1在职）',
    `start_date` DATE COMMENT '生效日期',
    `end_date` DATE COMMENT '失效日期'
) COMMENT '客服维度拉链表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dim/dim_customer_service_zip/'
TBLPROPERTIES ("parquet.compression"="snappy");

INSERT OVERWRITE TABLE dim_customer_service_zip PARTITION(dt='2025-08-04')
SELECT
    cs_id,
    cs_name,
    department,
    position,
    hire_date,
    status,
    start_date,
    end_date
FROM (
         WITH current_data AS (
             SELECT
                 cs_id,
                 cs_name,
                 COALESCE(department, '未知部门') AS department,
                 position,
                 TO_DATE(hire_date) AS hire_date,
                 status
             FROM ods_customer_service_info
             WHERE dt = '2025-08-04' AND status IN (0, 1)
         ),
              historical_data AS (
                  SELECT * FROM dim_customer_service_zip
                  WHERE dt = '2025-08-03' AND end_date = '9999-12-31'
              ),
              combined AS (
                  SELECT * FROM current_data
                  UNION ALL
                  SELECT cs_id, cs_name, department, position, hire_date, status
                  FROM historical_data
              ),
              changes AS (
                  SELECT
                      cs_id,
                      cs_name,
                      department,
                      position,
                      hire_date,
                      status,
                      LAG(status) OVER (PARTITION BY cs_id ORDER BY hire_date) AS prev_status,
                          LAG(department) OVER (PARTITION BY cs_id ORDER BY hire_date) AS prev_dept
                  FROM combined
              ),
              min_hire_dates AS (
                  SELECT cs_id, MIN(hire_date) AS min_hire_date
                  FROM changes
                  GROUP BY cs_id
              )
         SELECT
             c.cs_id,
             c.cs_name,
             c.department,
             c.position,
             c.hire_date,
             c.status,
             CASE
                 WHEN c.prev_status IS NULL THEN c.hire_date
                 WHEN c.status != c.prev_status OR c.department != c.prev_dept THEN DATE '2025-08-04'
            ELSE m.min_hire_date
        END AS start_date,
        CASE
            WHEN c.status != c.prev_status OR c.department != c.prev_dept THEN DATE '2025-08-03'
            ELSE DATE '9999-12-31'
        END AS end_date
         FROM changes c
             JOIN min_hire_dates m ON c.cs_id = m.cs_id
         WHERE c.status != 2
     ) t;

select  * from dim_customer_service_zip;
--二、优惠活动维度表
CREATE EXTERNAL TABLE dim_special_offer_activity (
    `activity_id` BIGINT COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `activity_level` STRING COMMENT '活动级别',
    `offer_type` STRING COMMENT '优惠类型',
    `start_date` DATE COMMENT '开始日期',
    `end_date` DATE COMMENT '结束日期',
    `max_custom_amount` DECIMAL(10,2) COMMENT '最大优惠金额'
) COMMENT '优惠活动维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dim/dim_special_offer_activity/'
TBLPROPERTIES ("parquet.compression"="snappy");


INSERT OVERWRITE TABLE dim_special_offer_activity PARTITION(dt='2025-08-04')
SELECT
    activity_id,
    activity_name,
    CASE activity_level
        WHEN 1 THEN '商品级'
        WHEN 2 THEN 'SKU级'
        ELSE '普通级'
        END AS activity_level,
    CASE offer_type
        WHEN 1 THEN '固定优惠'
        WHEN 2 THEN '自定义优惠'
        ELSE '其他'
        END AS offer_type,
    TO_DATE(start_time) AS start_date,
    TO_DATE(end_time) AS end_date,
    max_custom_amount
FROM ods_special_offer_activity
WHERE dt = '2025-08-04'
  AND status = 1 -- 仅有效活动
  AND TO_DATE(end_time) >= '2025-08-04';

select * from dim_special_offer_activity;
--三、活动商品维度表
CREATE EXTERNAL TABLE dim_special_offer_products (
    `product_id` BIGINT COMMENT '商品ID',
    `activity_id` BIGINT COMMENT '活动ID',
    `fixed_offer_amount` DECIMAL(10,2) COMMENT '固定优惠',
    `max_offer_amount` DECIMAL(10,2) COMMENT '最大优惠',
    `purchase_limit` INT COMMENT '购买限制'
) COMMENT '活动商品维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dim/dim_special_offer_products/'
TBLPROPERTIES ("parquet.compression"="snappy");


INSERT OVERWRITE TABLE dim_special_offer_products PARTITION(dt='2025-08-04')
SELECT
    p.product_id,
    p.activity_id,
    p.fixed_offer_amount,
    p.max_offer_amount,
    p.purchase_limit
FROM ods_special_offer_products p
WHERE p.dt = '2025-08-04'
  AND p.status = 1;

select * from dim_special_offer_products;
--四、活动SKU维度表
CREATE EXTERNAL TABLE dim_special_offer_skus (
    `sku_id` BIGINT COMMENT 'SKU ID',
    `product_id` BIGINT COMMENT '商品ID',
    `fixed_offer_amount` DECIMAL(10,2) COMMENT '固定优惠',
    `max_offer_amount` DECIMAL(10,2) COMMENT '最大优惠'
) COMMENT '活动SKU维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dim/dim_special_offer_skus/'
TBLPROPERTIES ("parquet.compression"="snappy");

INSERT OVERWRITE TABLE dim_special_offer_skus PARTITION(dt='2025-08-04')
SELECT
    sku_id,
    product_id,
    fixed_offer_amount,
    max_offer_amount
FROM ods_special_offer_skus
WHERE dt = '2025-08-04';

select * from dim_special_offer_skus;

--五、优惠发送记录维度表
CREATE EXTERNAL TABLE dim_offer_send_records (
    `record_id` BIGINT COMMENT '记录ID',
    `activity_id` BIGINT COMMENT '活动ID',
    `product_id` BIGINT COMMENT '商品ID',
    `sku_id` BIGINT COMMENT 'SKU ID',
    `cs_id` BIGINT COMMENT '客服ID',
    `offer_type` STRING COMMENT '优惠类型',
    `valid_hours` INT COMMENT '有效期(小时)'
) COMMENT '优惠发送记录维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dim/dim_offer_send_records/'
TBLPROPERTIES ("parquet.compression"="snappy");

INSERT OVERWRITE TABLE dim_offer_send_records PARTITION(dt='2025-08-04')
SELECT
    s.record_id,
    s.activity_id,
    s.product_id,
    s.sku_id,
    s.cs_id,
    CASE a.offer_type
        WHEN 1 THEN '固定优惠'
        WHEN 2 THEN '自定义优惠'
        ELSE '其他'
        END AS offer_type,
    s.valid_hours
FROM ods_offer_send_records s
         JOIN ods_special_offer_activity a
              ON s.activity_id = a.activity_id AND a.dt = '2025-08-04'
WHERE s.dt = '2025-08-04';

select * from dim_offer_send_records;
--六、优惠核销记录维度表
CREATE EXTERNAL TABLE dim_offer_redemption_records (
    `redemption_id` BIGINT COMMENT '核销ID',
    `record_id` BIGINT COMMENT '记录ID',
    `actual_offer_amount` DECIMAL(10,2) COMMENT '实际优惠金额',
    `payment_amount` DECIMAL(10,2) COMMENT '支付金额'
) COMMENT '优惠核销记录维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall_08/dim/dim_offer_redemption_records/'
TBLPROPERTIES ("parquet.compression"="snappy");

-- 工单编号: 大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
INSERT OVERWRITE TABLE dim_offer_redemption_records PARTITION(dt='2025-08-04')
SELECT
    redemption_id,
    record_id,
    actual_offer_amount,
    payment_amount
FROM ods_offer_redemption_records
WHERE dt = '2025-08-04';
select * from dim_offer_redemption_records;