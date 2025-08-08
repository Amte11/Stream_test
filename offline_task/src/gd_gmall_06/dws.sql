SET hive.exec.mode.local.auto=true;
USE gmall_06;

-- 1. DWS店铺新品监控表 - 1天周期
CREATE TABLE IF NOT EXISTS dws_store_new_sku_monitor_1d (
                                                            store_id STRING COMMENT '店铺ID',
                                                            store_name STRING COMMENT '店铺名称',
                                                            store_type STRING COMMENT '店铺类型',
                                                            platform STRING COMMENT '所属平台',
                                                            category_main STRING COMMENT '主营类目',
                                                            new_sku_count INT COMMENT '新品数量',
                                                            latest_putaway_time TIMESTAMP COMMENT '最新上架时间',
                                                            total_payment_amount DECIMAL(16,2) COMMENT '新品总支付金额',
    avg_days_to_first_payment DECIMAL(10,2) COMMENT '平均首次支付天数',
    payment_user_count INT COMMENT '支付用户数',
    avg_sale_price DECIMAL(10,2) COMMENT '新品平均售价'
    )
    COMMENT 'DWS层-店铺新品监控表(1天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_store_new_sku_monitor_1d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_store_new_sku_monitor_1d PARTITION(dt='2025-08-08')
SELECT
    s.store_id,
    s.store_name,
    s.store_type,
    s.platform,
    s.category_main,
    s.new_sku_count,
    s.latest_putaway_time,
    s.total_payment_amount,
    s.avg_days_to_first_payment,
    COALESCE(u.payment_user_count, 0) AS payment_user_count,
    COALESCE(p.avg_sale_price, 0) AS avg_sale_price
FROM dwd_store_new_sku_summary s
         LEFT JOIN (
    SELECT
        store_id,
        COUNT(DISTINCT user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
      AND days_after_putaway = 0  -- 仅当天数据
    GROUP BY store_id
) u ON s.store_id = u.store_id
         LEFT JOIN (
    SELECT
        store_id,
        ROUND(AVG(sale_price), 2) AS avg_sale_price
    FROM dwd_sku_new_arrival_info
    WHERE dt = '2025-08-08'
    GROUP BY store_id
) p ON s.store_id = p.store_id
WHERE s.dt = '2025-08-08';

-- 店铺新品监控表 - 7天周期
CREATE TABLE IF NOT EXISTS dws_store_new_sku_monitor_7d (
                                                            store_id STRING COMMENT '店铺ID',
                                                            store_name STRING COMMENT '店铺名称',
                                                            store_type STRING COMMENT '店铺类型',
                                                            platform STRING COMMENT '所属平台',
                                                            category_main STRING COMMENT '主营类目',
                                                            new_sku_count INT COMMENT '新品数量',
                                                            latest_putaway_time TIMESTAMP COMMENT '最新上架时间',
                                                            total_payment_amount DECIMAL(16,2) COMMENT '新品总支付金额',
    avg_days_to_first_payment DECIMAL(10,2) COMMENT '平均首次支付天数',
    payment_user_count INT COMMENT '支付用户数',
    avg_sale_price DECIMAL(10,2) COMMENT '新品平均售价'
    )
    COMMENT 'DWS层-店铺新品监控表(7天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_store_new_sku_monitor_7d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_store_new_sku_monitor_7d PARTITION(dt='2025-08-08')
SELECT
    s.store_id,
    s.store_name,
    s.store_type,
    s.platform,
    s.category_main,
    s.new_sku_count,
    s.latest_putaway_time,
    s.total_payment_amount,
    s.avg_days_to_first_payment,
    COALESCE(u.payment_user_count, 0) AS payment_user_count,
    COALESCE(p.avg_sale_price, 0) AS avg_sale_price
FROM dwd_store_new_sku_summary s
         LEFT JOIN (
    SELECT
        store_id,
        COUNT(DISTINCT user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
      AND days_after_putaway BETWEEN 0 AND 6  -- 7天内数据
    GROUP BY store_id
) u ON s.store_id = u.store_id
         LEFT JOIN (
    SELECT
        store_id,
        ROUND(AVG(sale_price), 2) AS avg_sale_price
    FROM dwd_sku_new_arrival_info
    WHERE dt = '2025-08-08'
    GROUP BY store_id
) p ON s.store_id = p.store_id
WHERE s.dt = '2025-08-08';

-- 店铺新品监控表 - 30天周期
CREATE TABLE IF NOT EXISTS dws_store_new_sku_monitor_30d (
                                                             store_id STRING COMMENT '店铺ID',
                                                             store_name STRING COMMENT '店铺名称',
                                                             store_type STRING COMMENT '店铺类型',
                                                             platform STRING COMMENT '所属平台',
                                                             category_main STRING COMMENT '主营类目',
                                                             new_sku_count INT COMMENT '新品数量',
                                                             latest_putaway_time TIMESTAMP COMMENT '最新上架时间',
                                                             total_payment_amount DECIMAL(16,2) COMMENT '新品总支付金额',
    avg_days_to_first_payment DECIMAL(10,2) COMMENT '平均首次支付天数',
    payment_user_count INT COMMENT '支付用户数',
    avg_sale_price DECIMAL(10,2) COMMENT '新品平均售价'
    )
    COMMENT 'DWS层-店铺新品监控表(30天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_store_new_sku_monitor_30d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_store_new_sku_monitor_30d PARTITION(dt='2025-08-08')
SELECT
    s.store_id,
    s.store_name,
    s.store_type,
    s.platform,
    s.category_main,
    s.new_sku_count,
    s.latest_putaway_time,
    s.total_payment_amount,
    s.avg_days_to_first_payment,
    COALESCE(u.payment_user_count, 0) AS payment_user_count,
    COALESCE(p.avg_sale_price, 0) AS avg_sale_price
FROM dwd_store_new_sku_summary s
         LEFT JOIN (
    SELECT
        store_id,
        COUNT(DISTINCT user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
      AND days_after_putaway BETWEEN 0 AND 29  -- 30天内数据
    GROUP BY store_id
) u ON s.store_id = u.store_id
         LEFT JOIN (
    SELECT
        store_id,
        ROUND(AVG(sale_price), 2) AS avg_sale_price
    FROM dwd_sku_new_arrival_info
    WHERE dt = '2025-08-08'
    GROUP BY store_id
) p ON s.store_id = p.store_id
WHERE s.dt = '2025-08-08';

-- 2. DWS新品列表汇总表 - 1天周期
CREATE TABLE IF NOT EXISTS dws_sku_new_list_summary_1d (
                                                           sku_id STRING COMMENT '商品货号',
                                                           sku_title STRING COMMENT '商品标题',
                                                           sku_main_image STRING COMMENT '主图URL',
                                                           category_name STRING COMMENT '类目名称',
                                                           brand_name STRING COMMENT '品牌名称',
                                                           store_name STRING COMMENT '店铺名称',
                                                           putaway_date DATE COMMENT '上架日期',
                                                           new_type STRING COMMENT '新品类型',
                                                           initial_sale_price DECIMAL(10,2) COMMENT '初始售价',
    total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    payment_quantity INT COMMENT '支付件数',
    payment_user_count INT COMMENT '支付用户数'
    )
    COMMENT 'DWS层-新品列表汇总表(1天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_sku_new_list_summary_1d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_sku_new_list_summary_1d PARTITION(dt='2025-08-08')
SELECT
    i.sku_id,
    i.sku_title,
    i.sku_main_image,
    i.category_name,
    i.brand_name,
    i.store_name,
    DATE(i.putaway_time) AS putaway_date,
    i.new_type,
    COALESCE(y.initial_sale_price, 0) AS initial_sale_price,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
    COALESCE(p.payment_quantity, 0) AS payment_quantity,
    COALESCE(p.payment_user_count, 0) AS payment_user_count
FROM dwd_sku_new_arrival_info i
    LEFT JOIN dwd_sku_yearly_new_info y
ON i.sku_id = y.sku_id AND i.dt = y.dt
    LEFT JOIN (
    SELECT
    sku_id,
    SUM(payment_amount) AS total_payment_amount,
    SUM(payment_quantity) AS payment_quantity,
    COUNT(DISTINCT user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    AND days_after_putaway = 0  -- 仅当天数据
    GROUP BY sku_id
    ) p ON i.sku_id = p.sku_id
WHERE i.dt = '2025-08-08';

-- 新品列表汇总表 - 7天周期
CREATE TABLE IF NOT EXISTS dws_sku_new_list_summary_7d (
                                                           sku_id STRING COMMENT '商品货号',
                                                           sku_title STRING COMMENT '商品标题',
                                                           sku_main_image STRING COMMENT '主图URL',
                                                           category_name STRING COMMENT '类目名称',
                                                           brand_name STRING COMMENT '品牌名称',
                                                           store_name STRING COMMENT '店铺名称',
                                                           putaway_date DATE COMMENT '上架日期',
                                                           new_type STRING COMMENT '新品类型',
                                                           initial_sale_price DECIMAL(10,2) COMMENT '初始售价',
    total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    payment_quantity INT COMMENT '支付件数',
    payment_user_count INT COMMENT '支付用户数'
    )
    COMMENT 'DWS层-新品列表汇总表(7天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_sku_new_list_summary_7d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_sku_new_list_summary_7d PARTITION(dt='2025-08-08')
SELECT
    i.sku_id,
    i.sku_title,
    i.sku_main_image,
    i.category_name,
    i.brand_name,
    i.store_name,
    DATE(i.putaway_time) AS putaway_date,
    i.new_type,
    COALESCE(y.initial_sale_price, 0) AS initial_sale_price,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
    COALESCE(p.payment_quantity, 0) AS payment_quantity,
    COALESCE(p.payment_user_count, 0) AS payment_user_count
FROM dwd_sku_new_arrival_info i
    LEFT JOIN dwd_sku_yearly_new_info y
ON i.sku_id = y.sku_id AND i.dt = y.dt
    LEFT JOIN (
    SELECT
    sku_id,
    SUM(payment_amount) AS total_payment_amount,
    SUM(payment_quantity) AS payment_quantity,
    COUNT(DISTINCT user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    AND days_after_putaway BETWEEN 0 AND 6  -- 7天内数据
    GROUP BY sku_id
    ) p ON i.sku_id = p.sku_id
WHERE i.dt = '2025-08-08';

-- 新品列表汇总表 - 30天周期
CREATE TABLE IF NOT EXISTS dws_sku_new_list_summary_30d (
                                                            sku_id STRING COMMENT '商品货号',
                                                            sku_title STRING COMMENT '商品标题',
                                                            sku_main_image STRING COMMENT '主图URL',
                                                            category_name STRING COMMENT '类目名称',
                                                            brand_name STRING COMMENT '品牌名称',
                                                            store_name STRING COMMENT '店铺名称',
                                                            putaway_date DATE COMMENT '上架日期',
                                                            new_type STRING COMMENT '新品类型',
                                                            initial_sale_price DECIMAL(10,2) COMMENT '初始售价',
    total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    payment_quantity INT COMMENT '支付件数',
    payment_user_count INT COMMENT '支付用户数'
    )
    COMMENT 'DWS层-新品列表汇总表(30天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_sku_new_list_summary_30d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_sku_new_list_summary_30d PARTITION(dt='2025-08-08')
SELECT
    i.sku_id,
    i.sku_title,
    i.sku_main_image,
    i.category_name,
    i.brand_name,
    i.store_name,
    DATE(i.putaway_time) AS putaway_date,
    i.new_type,
    COALESCE(y.initial_sale_price, 0) AS initial_sale_price,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
    COALESCE(p.payment_quantity, 0) AS payment_quantity,
    COALESCE(p.payment_user_count, 0) AS payment_user_count
FROM dwd_sku_new_arrival_info i
    LEFT JOIN dwd_sku_yearly_new_info y
ON i.sku_id = y.sku_id AND i.dt = y.dt
    LEFT JOIN (
    SELECT
    sku_id,
    SUM(payment_amount) AS total_payment_amount,
    SUM(payment_quantity) AS payment_quantity,
    COUNT(DISTINCT user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
    AND days_after_putaway BETWEEN 0 AND 29  -- 30天内数据
    GROUP BY sku_id
    ) p ON i.sku_id = p.sku_id
WHERE i.dt = '2025-08-08';

-- 3. DWS新品全年复盘表 - 固定为30天周期
CREATE TABLE IF NOT EXISTS dws_sku_yearly_review_30d (
                                                         putaway_date DATE COMMENT '上架日期',
                                                         putaway_year INT COMMENT '上架年份',
                                                         putaway_month INT COMMENT '上架月份',
                                                         putaway_quarter INT COMMENT '上架季度',
                                                         new_sku_count INT COMMENT '新品数量',
                                                         total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_initial_sale_price DECIMAL(10,2) COMMENT '平均初始售价',
    category_payment_dist STRING COMMENT '类目支付分布'
    )
    COMMENT 'DWS层-新品全年复盘表(30天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_sku_yearly_review_30d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_sku_yearly_review_30d PARTITION(dt='2025-08-08')
SELECT
    y.putaway_date,
    y.putaway_year,
    y.putaway_month,
    y.putaway_quarter,
    COUNT(DISTINCT y.sku_id) AS new_sku_count,
    SUM(y.total_payment_amount) AS total_payment_amount,
    ROUND(AVG(y.initial_sale_price), 2) AS avg_initial_sale_price,
    CONCAT_WS(';',
              COLLECT_SET(
                      CONCAT(c.category_name, ':', CAST(ROUND(c.category_payment_ratio * 100, 2) AS STRING, '%')
                          )
                  ) AS category_payment_dist
FROM dwd_sku_yearly_new_info y
LEFT JOIN (
    SELECT
        category_id,
        category_name,
        SUM(total_payment_amount) / (SELECT SUM(total_payment_amount)
                                     FROM dwd_sku_yearly_new_info
                                     WHERE dt = '2025-08-08') AS category_payment_ratio
    FROM dwd_sku_yearly_new_info
    WHERE dt = '2025-08-08'
    GROUP BY category_id, category_name
    HAVING SUM(total_payment_amount) > 0
) c ON y.category_id = c.category_id
WHERE y.dt = '2025-08-08'
GROUP BY y.putaway_date, y.putaway_year, y.putaway_month, y.putaway_quarter;

-- 4. DWS新品类目分析表 - 30天周期
CREATE TABLE IF NOT EXISTS dws_category_new_sku_analysis_30d (
                                                                 category_id STRING COMMENT '类目ID',
                                                                 category_name STRING COMMENT '类目名称',
                                                                 new_sku_count INT COMMENT '新品数量',
                                                                 total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_sale_price DECIMAL(10,2) COMMENT '平均售价',
    top_brand_id STRING COMMENT 'Top品牌ID',
    top_brand_name STRING COMMENT 'Top品牌名称',
    payment_user_count INT COMMENT '支付用户数',
    avg_payment_per_user DECIMAL(10,2) COMMENT '人均支付金额'
    )
    COMMENT 'DWS层-新品类目分析表(30天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_category_new_sku_analysis_30d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_category_new_sku_analysis_30d PARTITION(dt='2025-08-08')
SELECT
    c.category_id,
    c.category_name,
    c.new_sku_count,
    c.total_payment_amount,
    c.avg_sale_price,
    c.top_brand_id,
    c.top_brand_name,
    COALESCE(u.payment_user_count, 0) AS payment_user_count,
    CASE WHEN COALESCE(u.payment_user_count, 0) > 0
             THEN ROUND(c.total_payment_amount / u.payment_user_count, 2)
         ELSE 0
        END AS avg_payment_per_user
FROM dwd_category_new_sku_analysis c
         LEFT JOIN (
    SELECT
        i.category_id,
        COUNT(DISTINCT p.user_id) AS payment_user_count
    FROM dwd_sku_new_payment_detail p
             JOIN dwd_sku_new_arrival_info i ON p.sku_id = i.sku_id AND p.dt = i.dt
    WHERE p.dt = '2025-08-08'
      AND p.days_after_putaway BETWEEN 0 AND 29  -- 30天内数据
    GROUP BY i.category_id
) u ON c.category_id = u.category_id
WHERE c.dt = '2025-08-08';

-- 5. DWS新品渠道分析表 - 30天周期
CREATE TABLE IF NOT EXISTS dws_channel_new_sku_analysis_30d (
                                                                channel STRING COMMENT '支付渠道',
                                                                new_sku_count INT COMMENT '新品数量',
                                                                total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
    avg_payment_amount DECIMAL(10,2) COMMENT '笔均支付金额',
    payment_user_count INT COMMENT '支付用户数',
    avg_payment_per_user DECIMAL(10,2) COMMENT '人均支付金额'
    )
    COMMENT 'DWS层-新品渠道分析表(30天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_channel_new_sku_analysis_30d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_channel_new_sku_analysis_30d PARTITION(dt='2025-08-08')
SELECT
    c.channel,
    c.new_sku_count,
    c.total_payment_amount,
    c.avg_payment_amount,
    c.payment_user_count,
    CASE WHEN c.payment_user_count > 0
             THEN ROUND(c.total_payment_amount / c.payment_user_count, 2)
         ELSE 0
        END AS avg_payment_per_user
FROM dwd_channel_new_sku_analysis c
WHERE c.dt = '2025-08-08';

-- 6. DWS最近上新表 - 1天周期
CREATE TABLE IF NOT EXISTS dws_recent_new_arrival_1d (
                                                         sku_id STRING COMMENT '商品货号',
                                                         sku_title STRING COMMENT '商品标题',
                                                         sku_main_image STRING COMMENT '主图URL',
                                                         store_name STRING COMMENT '店铺名称',
                                                         putaway_time TIMESTAMP COMMENT '上架时间',
                                                         new_type STRING COMMENT '新品类型',
                                                         total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    rn INT COMMENT '排名'
    )
    COMMENT 'DWS层-最近上新表(1天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_recent_new_arrival_1d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_recent_new_arrival_1d PARTITION(dt='2025-08-08')
SELECT
    i.sku_id,
    i.sku_title,
    i.sku_main_image,
    i.store_name,
    i.putaway_time,
    i.new_type,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
    ROW_NUMBER() OVER (ORDER BY i.putaway_time DESC) AS rn
FROM dwd_sku_new_arrival_info i
         LEFT JOIN (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment_amount
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
      AND days_after_putaway = 0  -- 仅当天数据
    GROUP BY sku_id
) p ON i.sku_id = p.sku_id
WHERE i.dt = '2025-08-08'
ORDER BY i.putaway_time DESC
    LIMIT 20;

-- 最近上新表 - 7天周期
CREATE TABLE IF NOT EXISTS dws_recent_new_arrival_7d (
                                                         sku_id STRING COMMENT '商品货号',
                                                         sku_title STRING COMMENT '商品标题',
                                                         sku_main_image STRING COMMENT '主图URL',
                                                         store_name STRING COMMENT '店铺名称',
                                                         putaway_time TIMESTAMP COMMENT '上架时间',
                                                         new_type STRING COMMENT '新品类型',
                                                         total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    rn INT COMMENT '排名'
    )
    COMMENT 'DWS层-最近上新表(7天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_recent_new_arrival_7d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_recent_new_arrival_7d PARTITION(dt='2025-08-08')
SELECT
    i.sku_id,
    i.sku_title,
    i.sku_main_image,
    i.store_name,
    i.putaway_time,
    i.new_type,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
    ROW_NUMBER() OVER (ORDER BY i.putaway_time DESC) AS rn
FROM dwd_sku_new_arrival_info i
         LEFT JOIN (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment_amount
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
      AND days_after_putaway BETWEEN 0 AND 6  -- 7天内数据
    GROUP BY sku_id
) p ON i.sku_id = p.sku_id
WHERE i.dt = '2025-08-08'
ORDER BY i.putaway_time DESC
    LIMIT 20;

-- 最近上新表 - 30天周期
CREATE TABLE IF NOT EXISTS dws_recent_new_arrival_30d (
                                                          sku_id STRING COMMENT '商品货号',
                                                          sku_title STRING COMMENT '商品标题',
                                                          sku_main_image STRING COMMENT '主图URL',
                                                          store_name STRING COMMENT '店铺名称',
                                                          putaway_time TIMESTAMP COMMENT '上架时间',
                                                          new_type STRING COMMENT '新品类型',
                                                          total_payment_amount DECIMAL(16,2) COMMENT '30天累计支付金额',
    rn INT COMMENT '排名'
    )
    COMMENT 'DWS层-最近上新表(30天周期)'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/dws/dws_recent_new_arrival_30d'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE dws_recent_new_arrival_30d PARTITION(dt='2025-08-08')
SELECT
    i.sku_id,
    i.sku_title,
    i.sku_main_image,
    i.store_name,
    i.putaway_time,
    i.new_type,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
    ROW_NUMBER() OVER (ORDER BY i.putaway_time DESC) AS rn
FROM dwd_sku_new_arrival_info i
         LEFT JOIN (
    SELECT
        sku_id,
        SUM(payment_amount) AS total_payment_amount
    FROM dwd_sku_new_payment_detail
    WHERE dt = '2025-08-08'
      AND days_after_putaway BETWEEN 0 AND 29  -- 30天内数据
    GROUP BY sku_id
) p ON i.sku_id = p.sku_id
WHERE i.dt = '2025-08-08'
ORDER BY i.putaway_time DESC
    LIMIT 20;