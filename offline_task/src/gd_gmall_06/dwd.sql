-- SET hive.exec.mode.local.auto=true;  -- 移除该配置，避免生产环境风险

USE gmall_06;

-- 1. DWD新品基础信息表 (dwd_sku_new_arrival_info)
CREATE TABLE IF NOT EXISTS dwd_sku_new_arrival_info (
                                                        sku_id STRING COMMENT '商品货号',
                                                        sku_title STRING COMMENT '商品标题',
                                                        sku_main_image STRING COMMENT '主图URL',
                                                        category_id STRING COMMENT '类目ID',
                                                        category_name STRING COMMENT '类目名称',
                                                        brand_id STRING COMMENT '品牌ID',
                                                        brand_name STRING COMMENT '品牌名称',
                                                        store_id STRING COMMENT '店铺ID',
                                                        store_name STRING COMMENT '店铺名称',
                                                        putaway_time TIMESTAMP COMMENT '上架时间',
                                                        new_type STRING COMMENT '新品类型',
                                                        is_tmall_new TINYINT COMMENT '是否天猫新品',
                                                        new_tag_valid_days INT COMMENT '标签有效期(天)',
                                                        new_tag_start_time TIMESTAMP COMMENT '标签生效时间',
                                                        new_tag_end_time TIMESTAMP COMMENT '标签失效时间'
)
    COMMENT 'DWD层-新品基础信息表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_sku_new_arrival_info'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dwd_sku_new_arrival_info PARTITION(dt='2025-08-08')
SELECT
    b.sku_id,
    COALESCE(b.sku_title, '未知') AS sku_title,
    COALESCE(b.sku_main_image, '') AS sku_main_image,
    b.category_id,
    COALESCE(b.category_name, '其他') AS category_name,
    b.brand_id,
    COALESCE(b.brand_name, '其他') AS brand_name,
    b.store_id,
    COALESCE(b.store_name, '') AS store_name,
    COALESCE(t.putaway_time, b.putaway_time) AS putaway_time,
    COALESCE(t.new_type, '普通新品') AS new_type,
    COALESCE(t.is_tmall_new, 0) AS is_tmall_new,
    COALESCE(t.new_tag_valid_days, 30) AS new_tag_valid_days,
    t.new_tag_start_time,
    t.new_tag_end_time
FROM ods_sku_base_info b
         LEFT JOIN (
    SELECT
        sku_id,
        putaway_time,
        new_type,
        is_tmall_new,
        new_tag_valid_days,
        new_tag_start_time,
        new_tag_end_time,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY update_time DESC) AS rn
    FROM ods_sku_new_arrival_tag
    WHERE dt = '2025-08-08'
      AND new_tag_audit_status = '通过'
      AND new_tag_end_time >= '2025-08-08'
      AND is_tmall_new = 1  -- 提前过滤天猫新品
) t ON b.sku_id = t.sku_id AND t.rn = 1
WHERE b.dt = '2025-08-08'
  AND b.is_delete = 0
  AND b.is_online = 1
  AND t.sku_id IS NOT NULL;  -- 确保匹配到天猫新品

-- 2. DWD新品支付明细表 (dwd_sku_new_payment_detail)
CREATE TABLE IF NOT EXISTS dwd_sku_new_payment_detail (
                                                          payment_id STRING COMMENT '支付单ID',
                                                          order_id STRING COMMENT '订单ID',
                                                          sku_id STRING COMMENT '商品货号',
                                                          user_id STRING COMMENT '用户ID',
                                                          payment_amount DECIMAL(16,2) COMMENT '支付金额',
    payment_quantity INT COMMENT '支付数量',
    payment_time TIMESTAMP COMMENT '支付时间',
    putaway_time TIMESTAMP COMMENT '上架时间',
    days_after_putaway INT COMMENT '上架后天数',
    store_id STRING COMMENT '店铺ID',
    channel STRING COMMENT '支付渠道'
    )
    COMMENT 'DWD层-新品支付明细表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_sku_new_payment_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dwd_sku_new_payment_detail PARTITION(dt='2025-08-08')
SELECT
    p.payment_id,
    p.order_id,
    p.sku_id,
    p.user_id,
    p.payment_amount,
    p.payment_quantity,
    p.payment_time,
    n.putaway_time,
    DATEDIFF(p.payment_time, n.putaway_time) AS days_after_putaway,
    p.store_id,
    COALESCE(p.channel, '未知') AS channel
FROM ods_sku_payment_detail p
         JOIN dwd_sku_new_arrival_info n ON p.sku_id = n.sku_id
WHERE p.dt = '2025-08-08'
  AND p.payment_status = 1
  AND p.refund_status = 0
  AND DATEDIFF(p.payment_time, n.putaway_time) BETWEEN 0 AND 30;

-- 3. DWD年度新品信息表 (dwd_sku_yearly_new_info)
CREATE TABLE IF NOT EXISTS dwd_sku_yearly_new_info (
                                                       sku_id STRING COMMENT '商品货号',
                                                       putaway_date DATE COMMENT '上架日期',
                                                       putaway_year INT COMMENT '上架年份',
                                                       putaway_month INT COMMENT '上架月份',
                                                       putaway_quarter INT COMMENT '上架季度',
                                                       new_type STRING COMMENT '新品类型',
                                                       initial_sale_price DECIMAL(10,2) COMMENT '初始售价',
    category_id STRING COMMENT '类目ID',
    brand_id STRING COMMENT '品牌ID',
    store_id STRING COMMENT '店铺ID',
    total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    first_payment_date DATE COMMENT '首次支付日期'
    )
    COMMENT 'DWD层-年度新品信息表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_sku_yearly_new_info'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 提前聚合支付数据，避免重复子查询
WITH payment_summary AS (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment,
        MIN(DATE(payment_time)) AS first_payment_date
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    GROUP BY sku_id
)
INSERT OVERWRITE TABLE dwd_sku_yearly_new_info PARTITION(dt='2025-08-08')
SELECT
    y.sku_id,
    y.putaway_date,
    y.putaway_year,
    y.putaway_month,
    COALESCE(y.putaway_quarter,
             CASE
                 WHEN y.putaway_month BETWEEN 1 AND 3 THEN 1
                 WHEN y.putaway_month BETWEEN 4 AND 6 THEN 2
                 WHEN y.putaway_month BETWEEN 7 AND 9 THEN 3
                 ELSE 4
                 END) AS putaway_quarter,
    COALESCE(n.new_type, '普通新品') AS new_type,
    y.initial_sale_price,
    y.category_id,
    y.brand_id,
    b.store_id,
    COALESCE(p.total_payment, 0) AS total_payment_amount,
    COALESCE(p.first_payment_date, y.first_payment_date) AS first_payment_date
FROM ods_sku_yearly_new_record y
         LEFT JOIN (
    SELECT
        sku_id,
        new_type
    FROM ods_sku_new_arrival_tag
    WHERE dt = '2025-08-08'
      AND new_tag_audit_status = '通过'
      AND is_tmall_new = 1
) n ON y.sku_id = n.sku_id
         LEFT JOIN ods_sku_base_info b ON y.sku_id = b.sku_id AND b.dt = '2025-08-08'
         LEFT JOIN payment_summary p ON y.sku_id = p.sku_id
WHERE y.dt = '2025-08-08'
  AND y.putaway_date IS NOT NULL;

-- 4. DWD店铺新品汇总表 (dwd_store_new_sku_summary)
CREATE TABLE IF NOT EXISTS dwd_store_new_sku_summary (
                                                         store_id STRING COMMENT '店铺ID',
                                                         store_name STRING COMMENT '店铺名称',
                                                         store_type STRING COMMENT '店铺类型',
                                                         platform STRING COMMENT '所属平台',
                                                         category_main STRING COMMENT '主营类目',
                                                         new_sku_count INT COMMENT '新品数量',
                                                         latest_putaway_time TIMESTAMP COMMENT '最新上架时间',
                                                         total_payment_amount DECIMAL(16,2) COMMENT '新品总支付金额',
    avg_days_to_first_payment DECIMAL(10,2) COMMENT '平均首次支付天数'
    )
    COMMENT 'DWD层-店铺新品汇总表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_store_new_sku_summary'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 提前聚合支付数据
WITH payment_summary AS (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    GROUP BY sku_id
),
     first_payment_summary AS (
         SELECT
             sku_id,
             MIN(DATE(payment_time)) AS first_payment_date
         FROM dwd_sku_new_payment_detail
         WHERE dt = '2025-08-08'
         GROUP BY sku_id
     )
INSERT OVERWRITE TABLE dwd_store_new_sku_summary PARTITION(dt='2025-08-08')
SELECT
    s.store_id,
    COALESCE(s.store_name, '') AS store_name,
    COALESCE(s.store_type, '') AS store_type,
    COALESCE(s.platform, '未知') AS platform,
    COALESCE(s.category_main, '未分类') AS category_main,
    COUNT(DISTINCT n.sku_id) AS new_sku_count,
    MAX(n.putaway_time) AS latest_putaway_time,
    SUM(COALESCE(p.total_payment, 0)) AS total_payment_amount,
    ROUND(AVG(DATEDIFF(fp.first_payment_date, n.putaway_date)), 2) AS avg_days_to_first_payment
FROM ods_store_base_info s
         LEFT JOIN dwd_sku_new_arrival_info n
                   ON s.store_id = n.store_id AND n.dt = '2025-08-08'
         LEFT JOIN payment_summary p
                   ON n.sku_id = p.sku_id AND p.dt = '2025-08-08'
         LEFT JOIN first_payment_summary fp
                   ON n.sku_id = fp.sku_id AND fp.dt = '2025-08-08'
WHERE s.dt = '2025-08-08'
  AND fp.first_payment_date IS NOT NULL
  AND n.putaway_date IS NOT NULL
GROUP BY s.store_id, s.store_name, s.store_type, s.platform, s.category_main;

-- 5. DWD新品类目分析表 (dwd_category_new_sku_analysis)
CREATE TABLE IF NOT EXISTS dwd_category_new_sku_analysis (
                                                             category_id STRING COMMENT '类目ID',
                                                             category_name STRING COMMENT '类目名称',
                                                             new_sku_count INT COMMENT '新品数量',
                                                             total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_sale_price DECIMAL(10,2) COMMENT '平均售价',
    top_brand_id STRING COMMENT 'Top品牌ID',
    top_brand_name STRING COMMENT 'Top品牌名称'
    )
    COMMENT 'DWD层-新品类目分析表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_category_new_sku_analysis'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

WITH category_stats AS (
    SELECT
        n.category_id,
        n.category_name,
        COUNT(DISTINCT n.sku_id) AS new_sku_count,
        SUM(COALESCE(p.total_payment, 0)) AS total_payment_amount,
        ROUND(AVG(n.sale_price), 2) AS avg_sale_price
    FROM dwd_sku_new_arrival_info n
             LEFT JOIN (
        SELECT
            sku_id,
            SUM(payment_amount) AS total_payment
        FROM dwd_sku_new_payment_detail
        WHERE dt = '2025-08-08'
        GROUP BY sku_id
    ) p ON n.sku_id = p.sku_id
    WHERE n.dt = '2025-08-08'
    GROUP BY n.category_id, n.category_name
),
     brand_ranking AS (
         SELECT
             category_id,
             brand_id,
             brand_name,
             ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY COUNT(DISTINCT sku_id) DESC) AS rn
         FROM dwd_sku_new_arrival_info
         WHERE dt = '2025-08-08'
         GROUP BY category_id, brand_id, brand_name
     )
INSERT OVERWRITE TABLE dwd_category_new_sku_analysis PARTITION(dt='2025-08-08')
SELECT
    c.category_id,
    c.category_name,
    c.new_sku_count,
    c.total_payment_amount,
    c.avg_sale_price,
    b.brand_id AS top_brand_id,
    COALESCE(b.brand_name, '') AS top_brand_name
FROM category_stats c
         LEFT JOIN brand_ranking b ON c.category_id = b.category_id AND b.rn = 1;

-- 6. DWD新品渠道分析表 (dwd_channel_new_sku_analysis)
CREATE TABLE IF NOT EXISTS dwd_channel_new_sku_analysis (
                                                            channel STRING COMMENT '支付渠道',
                                                            new_sku_count INT COMMENT '新品数量',
                                                            total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_payment_amount DECIMAL(10,2) COMMENT '笔均支付金额',
    payment_user_count INT COMMENT '支付用户数'
    )
    COMMENT 'DWD层-新品渠道分析表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_channel_new_sku_analysis'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dwd_channel_new_sku_analysis PARTITION(dt='2025-08-08')
SELECT
    COALESCE(p.channel, '未知') AS channel,
    COUNT(DISTINCT p.sku_id) AS new_sku_count,
    SUM(p.payment_amount) AS total_payment_amount,
    ROUND(SUM(p.payment_amount) / COUNT(DISTINCT p.payment_id), 2) AS avg_payment_amount,
    COUNT(DISTINCT p.user_id) AS payment_user_count
FROM dwd_sku_new_payment_detail p
WHERE p.dt = '2025-08-08'
GROUP BY p.channel;
-- SET hive.exec.mode.local.auto=true;  -- 移除该配置，避免生产环境风险

USE gmall_06;

-- 1. DWD新品基础信息表 (dwd_sku_new_arrival_info)
CREATE TABLE IF NOT EXISTS dwd_sku_new_arrival_info (
                                                        sku_id STRING COMMENT '商品货号',
                                                        sku_title STRING COMMENT '商品标题',
                                                        sku_main_image STRING COMMENT '主图URL',
                                                        category_id STRING COMMENT '类目ID',
                                                        category_name STRING COMMENT '类目名称',
                                                        brand_id STRING COMMENT '品牌ID',
                                                        brand_name STRING COMMENT '品牌名称',
                                                        store_id STRING COMMENT '店铺ID',
                                                        store_name STRING COMMENT '店铺名称',
                                                        putaway_time TIMESTAMP COMMENT '上架时间',
                                                        new_type STRING COMMENT '新品类型',
                                                        is_tmall_new TINYINT COMMENT '是否天猫新品',
                                                        new_tag_valid_days INT COMMENT '标签有效期(天)',
                                                        new_tag_start_time TIMESTAMP COMMENT '标签生效时间',
                                                        new_tag_end_time TIMESTAMP COMMENT '标签失效时间'
)
    COMMENT 'DWD层-新品基础信息表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_sku_new_arrival_info'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dwd_sku_new_arrival_info PARTITION(dt='2025-08-08')
SELECT
    b.sku_id,
    COALESCE(b.sku_title, '未知') AS sku_title,
    COALESCE(b.sku_main_image, '') AS sku_main_image,
    b.category_id,
    COALESCE(b.category_name, '其他') AS category_name,
    b.brand_id,
    COALESCE(b.brand_name, '其他') AS brand_name,
    b.store_id,
    COALESCE(b.store_name, '') AS store_name,
    COALESCE(t.putaway_time, b.putaway_time) AS putaway_time,
    COALESCE(t.new_type, '普通新品') AS new_type,
    COALESCE(t.is_tmall_new, 0) AS is_tmall_new,
    COALESCE(t.new_tag_valid_days, 30) AS new_tag_valid_days,
    t.new_tag_start_time,
    t.new_tag_end_time
FROM ods_sku_base_info b
         LEFT JOIN (
    SELECT
        sku_id,
        putaway_time,
        new_type,
        is_tmall_new,
        new_tag_valid_days,
        new_tag_start_time,
        new_tag_end_time,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY update_time DESC) AS rn
    FROM ods_sku_new_arrival_tag
    WHERE dt = '2025-08-08'
      AND new_tag_audit_status = '通过'
      AND new_tag_end_time >= '2025-08-08'
      AND is_tmall_new = 1  -- 提前过滤天猫新品
) t ON b.sku_id = t.sku_id AND t.rn = 1
WHERE b.dt = '2025-08-08'
  AND b.is_delete = 0
  AND b.is_online = 1
  AND t.sku_id IS NOT NULL;  -- 确保匹配到天猫新品

-- 2. DWD新品支付明细表 (dwd_sku_new_payment_detail)
CREATE TABLE IF NOT EXISTS dwd_sku_new_payment_detail (
                                                          payment_id STRING COMMENT '支付单ID',
                                                          order_id STRING COMMENT '订单ID',
                                                          sku_id STRING COMMENT '商品货号',
                                                          user_id STRING COMMENT '用户ID',
                                                          payment_amount DECIMAL(16,2) COMMENT '支付金额',
    payment_quantity INT COMMENT '支付数量',
    payment_time TIMESTAMP COMMENT '支付时间',
    putaway_time TIMESTAMP COMMENT '上架时间',
    days_after_putaway INT COMMENT '上架后天数',
    store_id STRING COMMENT '店铺ID',
    channel STRING COMMENT '支付渠道'
    )
    COMMENT 'DWD层-新品支付明细表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_sku_new_payment_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dwd_sku_new_payment_detail PARTITION(dt='2025-08-08')
SELECT
    p.payment_id,
    p.order_id,
    p.sku_id,
    p.user_id,
    p.payment_amount,
    p.payment_quantity,
    p.payment_time,
    n.putaway_time,
    DATEDIFF(p.payment_time, n.putaway_time) AS days_after_putaway,
    p.store_id,
    COALESCE(p.channel, '未知') AS channel
FROM ods_sku_payment_detail p
         JOIN dwd_sku_new_arrival_info n ON p.sku_id = n.sku_id
WHERE p.dt = '2025-08-08'
  AND p.payment_status = 1
  AND p.refund_status = 0
  AND DATEDIFF(p.payment_time, n.putaway_time) BETWEEN 0 AND 30;

-- 3. DWD年度新品信息表 (dwd_sku_yearly_new_info)
CREATE TABLE IF NOT EXISTS dwd_sku_yearly_new_info (
                                                       sku_id STRING COMMENT '商品货号',
                                                       putaway_date DATE COMMENT '上架日期',
                                                       putaway_year INT COMMENT '上架年份',
                                                       putaway_month INT COMMENT '上架月份',
                                                       putaway_quarter INT COMMENT '上架季度',
                                                       new_type STRING COMMENT '新品类型',
                                                       initial_sale_price DECIMAL(10,2) COMMENT '初始售价',
    category_id STRING COMMENT '类目ID',
    brand_id STRING COMMENT '品牌ID',
    store_id STRING COMMENT '店铺ID',
    total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    first_payment_date DATE COMMENT '首次支付日期'
    )
    COMMENT 'DWD层-年度新品信息表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_sku_yearly_new_info'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 提前聚合支付数据，避免重复子查询
WITH payment_summary AS (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment,
        MIN(DATE(payment_time)) AS first_payment_date
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    GROUP BY sku_id
)
INSERT OVERWRITE TABLE dwd_sku_yearly_new_info PARTITION(dt='2025-08-08')
SELECT
    y.sku_id,
    y.putaway_date,
    y.putaway_year,
    y.putaway_month,
    COALESCE(y.putaway_quarter,
             CASE
                 WHEN y.putaway_month BETWEEN 1 AND 3 THEN 1
                 WHEN y.putaway_month BETWEEN 4 AND 6 THEN 2
                 WHEN y.putaway_month BETWEEN 7 AND 9 THEN 3
                 ELSE 4
                 END) AS putaway_quarter,
    COALESCE(n.new_type, '普通新品') AS new_type,
    y.initial_sale_price,
    y.category_id,
    y.brand_id,
    b.store_id,
    COALESCE(p.total_payment, 0) AS total_payment_amount,
    COALESCE(p.first_payment_date, y.first_payment_date) AS first_payment_date
FROM ods_sku_yearly_new_record y
         LEFT JOIN (
    SELECT
        sku_id,
        new_type
    FROM ods_sku_new_arrival_tag
    WHERE dt = '2025-08-08'
      AND new_tag_audit_status = '通过'
      AND is_tmall_new = 1
) n ON y.sku_id = n.sku_id
         LEFT JOIN ods_sku_base_info b ON y.sku_id = b.sku_id AND b.dt = '2025-08-08'
         LEFT JOIN payment_summary p ON y.sku_id = p.sku_id
WHERE y.dt = '2025-08-08'
  AND y.putaway_date IS NOT NULL;

-- 4. DWD店铺新品汇总表 (dwd_store_new_sku_summary)
CREATE TABLE IF NOT EXISTS dwd_store_new_sku_summary (
                                                         store_id STRING COMMENT '店铺ID',
                                                         store_name STRING COMMENT '店铺名称',
                                                         store_type STRING COMMENT '店铺类型',
                                                         platform STRING COMMENT '所属平台',
                                                         category_main STRING COMMENT '主营类目',
                                                         new_sku_count INT COMMENT '新品数量',
                                                         latest_putaway_time TIMESTAMP COMMENT '最新上架时间',
                                                         total_payment_amount DECIMAL(16,2) COMMENT '新品总支付金额',
    avg_days_to_first_payment DECIMAL(10,2) COMMENT '平均首次支付天数'
    )
    COMMENT 'DWD层-店铺新品汇总表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_store_new_sku_summary'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 提前聚合支付数据
WITH payment_summary AS (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    GROUP BY sku_id
),
     first_payment_summary AS (
         SELECT
             sku_id,
             MIN(DATE(payment_time)) AS first_payment_date
         FROM dwd_sku_new_payment_detail
         WHERE dt = '2025-08-08'
         GROUP BY sku_id
     )
INSERT OVERWRITE TABLE dwd_store_new_sku_summary PARTITION(dt='2025-08-08')
SELECT
    s.store_id,
    COALESCE(s.store_name, '') AS store_name,
    COALESCE(s.store_type, '') AS store_type,
    COALESCE(s.platform, '未知') AS platform,
    COALESCE(s.category_main, '未分类') AS category_main,
    COUNT(DISTINCT n.sku_id) AS new_sku_count,
    MAX(n.putaway_time) AS latest_putaway_time,
    SUM(COALESCE(p.total_payment, 0)) AS total_payment_amount,
    ROUND(AVG(DATEDIFF(fp.first_payment_date, n.putaway_date)), 2) AS avg_days_to_first_payment
FROM ods_store_base_info s
         LEFT JOIN dwd_sku_new_arrival_info n
                   ON s.store_id = n.store_id AND n.dt = '2025-08-08'
         LEFT JOIN payment_summary p
                   ON n.sku_id = p.sku_id AND p.dt = '2025-08-08'
         LEFT JOIN first_payment_summary fp
                   ON n.sku_id = fp.sku_id AND fp.dt = '2025-08-08'
WHERE s.dt = '2025-08-08'
  AND fp.first_payment_date IS NOT NULL
  AND n.putaway_date IS NOT NULL
GROUP BY s.store_id, s.store_name, s.store_type, s.platform, s.category_main;

-- 5. DWD新品类目分析表 (dwd_category_new_sku_analysis)
CREATE TABLE IF NOT EXISTS dwd_category_new_sku_analysis (
                                                             category_id STRING COMMENT '类目ID',
                                                             category_name STRING COMMENT '类目名称',
                                                             new_sku_count INT COMMENT '新品数量',
                                                             total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_sale_price DECIMAL(10,2) COMMENT '平均售价',
    top_brand_id STRING COMMENT 'Top品牌ID',
    top_brand_name STRING COMMENT 'Top品牌名称'
    )
    COMMENT 'DWD层-新品类目分析表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_category_new_sku_analysis'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

WITH category_stats AS (
    SELECT
        n.category_id,
        n.category_name,
        COUNT(DISTINCT n.sku_id) AS new_sku_count,
        SUM(COALESCE(p.total_payment, 0)) AS total_payment_amount,
        ROUND(AVG(n.sale_price), 2) AS avg_sale_price
    FROM dwd_sku_new_arrival_info n
             LEFT JOIN (
        SELECT
            sku_id,
            SUM(payment_amount) AS total_payment
        FROM dwd_sku_new_payment_detail
        WHERE dt = '2025-08-08'
        GROUP BY sku_id
    ) p ON n.sku_id = p.sku_id
    WHERE n.dt = '2025-08-08'
    GROUP BY n.category_id, n.category_name
),
     brand_ranking AS (
         SELECT
             category_id,
             brand_id,
             brand_name,
             ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY COUNT(DISTINCT sku_id) DESC) AS rn
         FROM dwd_sku_new_arrival_info
         WHERE dt = '2025-08-08'
         GROUP BY category_id, brand_id, brand_name
     )
INSERT OVERWRITE TABLE dwd_category_new_sku_analysis PARTITION(dt='2025-08-08')
SELECT
    c.category_id,
    c.category_name,
    c.new_sku_count,
    c.total_payment_amount,
    c.avg_sale_price,
    b.brand_id AS top_brand_id,
    COALESCE(b.brand_name, '') AS top_brand_name
FROM category_stats c
         LEFT JOIN brand_ranking b ON c.category_id = b.category_id AND b.rn = 1;

-- 6. DWD新品渠道分析表 (dwd_channel_new_sku_analysis)
CREATE TABLE IF NOT EXISTS dwd_channel_new_sku_analysis (
                                                            channel STRING COMMENT '支付渠道',
                                                            new_sku_count INT COMMENT '新品数量',
                                                            total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_payment_amount DECIMAL(10,2) COMMENT '笔均支付金额',
    payment_user_count INT COMMENT '支付用户数'
    )
    COMMENT 'DWD层-新品渠道分析表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dwd/dwd_channel_new_sku_analysis'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dwd_channel_new_sku_analysis PARTITION(dt='2025-08-08')
SELECT
    COALESCE(p.channel, '未知') AS channel,
    COUNT(DISTINCT p.sku_id) AS new_sku_count,
    SUM(p.payment_amount) AS total_payment_amount,
    ROUND(SUM(p.payment_amount) / COUNT(DISTINCT p.payment_id), 2) AS avg_payment_amount,
    COUNT(DISTINCT p.user_id) AS payment_user_count
FROM dwd_sku_new_payment_detail p
WHERE p.dt = '2025-08-08'
GROUP BY p.channel;
