from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import sys
import os


def main():
    # 1. 初始化SparkSession（关键参数前置配置）
    spark = SparkSession.builder \
        .appName("Hive_Dimension_Load") \
        .master("local[*]") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.shuffle.partitions", "4") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")  # 减少冗余日志，只显示警告和错误

    # 2. 处理日期变量（支持命令行传参或默认值）
    if len(sys.argv) > 1:
        date = sys.argv[1]  # 命令行传参：python script.py 2025-07-12
    else:
        date = "2025-07-12"  # 替换为实际业务日期（必须是源表存在的dt分区）
    print(f"===== 执行日期: 2025-07-12 =====")

    # 3. 切换数据库
    spark.sql("use tms01;")

    # 4. 封装插入+校验函数（减少重复代码，增加错误捕获）
    def load_and_verify(target_table, insert_sql):
        try:
            # 执行插入
            spark.sql(insert_sql)
            # 校验结果
            count_df = spark.sql(f"select count(*) from {target_table} where dt = '{date}'")
            count = count_df.collect()[0][0]
            print(f"✅ {target_table} 插入成功，数据条数: {count}")
        except AnalysisException as e:
            print(f"❌ {target_table} 元数据/语法错误: {str(e)}")
            print(f"失败SQL: {insert_sql[:500]}...")  # 打印部分SQL便于调试
        except Exception as e:
            print(f"❌ {target_table} 运行时错误: {str(e)}")
        # 5.1 车辆类型维度表：ads_city_stats

    insert_sql = f"""
    INSERT INTO ads_city_stats
    SELECT 
        '{date}' AS dt,
        recent_days,
        city_id,
        city_name,
        COALESCE(SUM(order_count), 0) AS order_count,
        COALESCE(SUM(order_amount), 0) AS order_amount,
        COALESCE(SUM(trans_finish_count), 0) AS trans_finish_count,
        COALESCE(SUM(trans_finish_distance), 0) AS trans_finish_distance,
        COALESCE(SUM(trans_finish_dur_sec), 0) AS trans_finish_dur_sec,
        CASE WHEN COALESCE(SUM(trans_finish_count), 0) = 0 THEN 0 
             ELSE COALESCE(SUM(trans_finish_distance), 0) / COALESCE(SUM(trans_finish_count), 0) 
        END AS avg_trans_finish_distance,
        CASE WHEN COALESCE(SUM(trans_finish_count), 0) = 0 THEN 0 
             ELSE COALESCE(SUM(trans_finish_dur_sec), 0) / COALESCE(SUM(trans_finish_count), 0) 
        END AS avg_trans_finish_dur_sec
    FROM (
        -- 1天数据部分
        SELECT 
            1 AS recent_days,
            city_id,
            city_name,
            order_count,
            order_amount,
            0 AS trans_finish_count,
            0 AS trans_finish_distance,
            0 AS trans_finish_dur_sec
        FROM dws_trade_org_cargo_type_order_1d
        WHERE dt = '{date}'
        
        UNION ALL
        
        SELECT 
            1 AS recent_days,
            COALESCE(city_for_level1.id, city_for_level2.id) AS city_id,
            COALESCE(city_for_level1.name, city_for_level2.name) AS city_name,
            0 AS order_count,
            0 AS order_amount,
            trans_finish_count,
            trans_finish_distance,
            trans_finish_dur_sec
        FROM dws_trans_org_truck_model_type_trans_finish_1d t
        LEFT JOIN dim_organ_full o ON t.org_id = o.id AND o.dt = '{date}'
        LEFT JOIN dim_region_full city_for_level1 
            ON o.region_id = city_for_level1.id AND city_for_level1.dt = '{date}'
        LEFT JOIN dim_region_full city_for_level2 
            ON city_for_level1.parent_id = city_for_level2.id AND city_for_level2.dt = '{date}'
        WHERE t.dt = '{date}'
        
        UNION ALL
        
        -- 多天数据部分
        SELECT 
            recent_days,
            city_id,
            city_name,
            order_count,
            order_amount,
            0 AS trans_finish_count,
            0 AS trans_finish_distance,
            0 AS trans_finish_dur_sec
        FROM dws_trade_org_cargo_type_order_nd
        WHERE dt = '{date}'
        
        UNION ALL
        
        SELECT 
            recent_days,
            city_id,
            city_name,
            0 AS order_count,
            0 AS order_amount,
            trans_finish_count,
            trans_finish_distance,
            trans_finish_dur_sec
        FROM dws_trans_shift_trans_finish_nd
        WHERE dt = '{date}'
    ) combined_data
    GROUP BY recent_days, city_id, city_name;
       """
    load_and_verify("ads_city_stats", insert_sql)

    # 5.2 车辆类型维度表：ads_driver_stats
    insert_sql = f"""
    INSERT INTO ads_driver_stats
    SELECT 
        '{date}' AS dt,
        recent_days,
        driver_id,
        driver_name,
        SUM(trans_finish_count) AS trans_finish_count,
        SUM(trans_finish_distance) AS trans_finish_distance,
        SUM(trans_finish_dur_sec) AS trans_finish_dur_sec,
        CASE WHEN SUM(trans_finish_count) = 0 THEN 0 
             ELSE SUM(trans_finish_distance) / SUM(trans_finish_count) 
        END AS avg_trans_finish_distance,
        CASE WHEN SUM(trans_finish_count) = 0 THEN 0 
             ELSE SUM(trans_finish_dur_sec) / SUM(trans_finish_count) 
        END AS avg_trans_finish_dur_sec,
        SUM(trans_finish_delay_count) AS trans_finish_late_count
    FROM (
        SELECT 
            recent_days,
            driver1_emp_id AS driver_id,
            driver1_name AS driver_name,
            trans_finish_count,
            trans_finish_distance,
            trans_finish_dur_sec,
            trans_finish_delay_count
        FROM dws_trans_shift_trans_finish_nd
        WHERE dt = '{date}' 
          AND driver2_emp_id IS NULL
        UNION ALL
        SELECT 
            recent_days,
            driver1_emp_id AS driver_id,
            driver1_name AS driver_name,
            trans_finish_count / 2.0 AS trans_finish_count,
            trans_finish_distance / 2.0 AS trans_finish_distance,
            trans_finish_dur_sec / 2.0 AS trans_finish_dur_sec,
            trans_finish_delay_count / 2.0 AS trans_finish_delay_count
        FROM dws_trans_shift_trans_finish_nd
        WHERE dt = '{date}' 
          AND driver2_emp_id IS NOT NULL
        UNION ALL
        SELECT 
            recent_days,
            driver2_emp_id AS driver_id,
            driver2_name AS driver_name,
            trans_finish_count / 2.0 AS trans_finish_count,
            trans_finish_distance / 2.0 AS trans_finish_distance,
            trans_finish_dur_sec / 2.0 AS trans_finish_dur_sec,
            trans_finish_delay_count / 2.0 AS trans_finish_delay_count
        FROM dws_trans_shift_trans_finish_nd
        WHERE dt = '{date}' 
          AND driver2_emp_id IS NOT NULL
    ) driver_data
    GROUP BY recent_days, driver_id, driver_name;
       """
    load_and_verify("ads_driver_stats", insert_sql)

    # 5.3 车辆类型维度表：ads_express_city_stats
    insert_sql = f"""
    INSERT INTO ads_express_city_stats
    SELECT 
        '{date}' AS dt,
        recent_days,
        city_id,
        city_name,
        COALESCE(SUM(receive_order_count), 0) AS receive_order_count,
        COALESCE(SUM(receive_order_amount), 0) AS receive_order_amount,
        COALESCE(SUM(deliver_suc_count), 0) AS deliver_suc_count,
        COALESCE(SUM(sort_count), 0) AS sort_count
    FROM (
        SELECT 
            1 AS recent_days,
            city_id,
            city_name,
            order_count AS receive_order_count,
            order_amount AS receive_order_amount,
            0 AS deliver_suc_count,
            0 AS sort_count
        FROM dws_trans_org_receive_1d
        WHERE dt = '{date}'
        
        UNION ALL
        -- 1天数据：派送成功
        SELECT 
            1 AS recent_days,
            city_id,
            city_name,
            0 AS receive_order_count,
            0 AS receive_order_amount,
            order_count AS deliver_suc_count,
            0 AS sort_count
        FROM dws_trans_org_deliver_suc_1d
        WHERE dt = '{date}'
        
        UNION ALL
        -- 1天数据：分拣
        SELECT 
            1 AS recent_days,
            city_id,
            city_name,
            0 AS receive_order_count,
            0 AS receive_order_amount,
            0 AS deliver_suc_count,
            sort_count
        FROM dws_trans_org_sort_1d
        WHERE dt = '{date}'
        
        UNION ALL
        -- 多天数据：收件
        SELECT 
            recent_days,
            city_id,
            city_name,
            order_count AS receive_order_count,
            order_amount AS receive_order_amount,
            0 AS deliver_suc_count,
            0 AS sort_count
        FROM dws_trans_org_receive_nd
        WHERE dt = '{date}'
        
        UNION ALL
        -- 多天数据：派送成功
        SELECT 
            recent_days,
            city_id,
            city_name,
            0 AS receive_order_count,
            0 AS receive_order_amount,
            order_count AS deliver_suc_count,
            0 AS sort_count
        FROM dws_trans_org_deliver_suc_nd
        WHERE dt = '{date}'
        
        UNION ALL
        
        -- 多天数据：分拣
        SELECT 
            recent_days,
            city_id,
            city_name,
            0 AS receive_order_count,
            0 AS receive_order_amount,
            0 AS deliver_suc_count,
            sort_count
        FROM dws_trans_org_sort_nd
        WHERE dt = '{date}'
    ) combined_data
    GROUP BY recent_days, city_id, city_name;
       """
    load_and_verify("ads_express_city_stats", insert_sql)

    # 5.4 车辆类型维度表：ads_express_org_stats
    insert_sql = f"""
    INSERT INTO ads_express_org_stats
    -- 当日数据统计
    SELECT 
        '{date}' AS dt,
        1 AS recent_days,
        COALESCE(d.org_id, s.org_id, r.org_id) AS org_id,
        COALESCE(d.org_name, s.org_name, r.org_name) AS org_name,
        COALESCE(r.receive_order_count, 0) AS receive_order_count,
        COALESCE(r.receive_order_amount, 0) AS receive_order_amount,
        COALESCE(d.deliver_suc_count, 0) AS deliver_suc_count,
        COALESCE(s.sort_count, 0) AS sort_count
    FROM (
        SELECT org_id, org_name, SUM(order_count) AS deliver_suc_count
        FROM dws_trans_org_deliver_suc_1d
        WHERE dt = '{date}'
        GROUP BY org_id, org_name
    ) d
    FULL OUTER JOIN (
        SELECT org_id, org_name, SUM(sort_count) AS sort_count
        FROM dws_trans_org_sort_1d
        WHERE dt = '{date}'
        GROUP BY org_id, org_name
    ) s ON d.org_id = s.org_id AND d.org_name = s.org_name
    FULL OUTER JOIN (
        SELECT org_id, org_name, 
               SUM(order_count) AS receive_order_count,
               SUM(order_amount) AS receive_order_amount
        FROM dws_trans_org_receive_1d
        WHERE dt = '{date}'
        GROUP BY org_id, org_name
    ) r ON COALESCE(d.org_id, s.org_id) = r.org_id 
       AND COALESCE(d.org_name, s.org_name) = r.org_name
    
    UNION ALL
    
    -- 多日周期数据统计
    SELECT 
        '{date}' AS dt,
        COALESCE(d.recent_days, s.recent_days, r.recent_days) AS recent_days,
        COALESCE(d.org_id, s.org_id, r.org_id) AS org_id,
        COALESCE(d.org_name, s.org_name, r.org_name) AS org_name,
        COALESCE(r.receive_order_count, 0) AS receive_order_count,
        COALESCE(r.receive_order_amount, 0) AS receive_order_amount,
        COALESCE(d.deliver_suc_count, 0) AS deliver_suc_count,
        COALESCE(s.sort_count, 0) AS sort_count
    FROM (
        SELECT recent_days, org_id, org_name, SUM(order_count) AS deliver_suc_count
        FROM dws_trans_org_deliver_suc_nd
        WHERE dt = '{date}'
        GROUP BY recent_days, org_id, org_name
    ) d
    FULL OUTER JOIN (
        SELECT recent_days, org_id, org_name, SUM(sort_count) AS sort_count
        FROM dws_trans_org_sort_nd
        WHERE dt = '{date}'
        GROUP BY recent_days, org_id, org_name
    ) s ON d.recent_days = s.recent_days AND d.org_id = s.org_id AND d.org_name = s.org_name
    FULL OUTER JOIN (
        SELECT recent_days, org_id, org_name,
               SUM(order_count) AS receive_order_count,
               SUM(order_amount) AS receive_order_amount
        FROM dws_trans_org_receive_nd
        WHERE dt = '{date}'
        GROUP BY recent_days, org_id, org_name
    ) r ON COALESCE(d.recent_days, s.recent_days) = r.recent_days 
       AND COALESCE(d.org_id, s.org_id) = r.org_id 
       AND COALESCE(d.org_name, s.org_name) = r.org_name;

       """
    load_and_verify("ads_express_province_stats", insert_sql)

    # 5.6 车辆类型维度表：ads_express_stats
    insert_sql = f"""
    INSERT INTO ads_express_stats
    SELECT 
        COALESCE(deliver.dt, sort.dt) AS dt,
        COALESCE(deliver.recent_days, sort.recent_days) AS recent_days,
        NVL(deliver_suc_count, 0) AS deliver_suc_count,
        NVL(sort_count, 0) AS sort_count
    FROM 
    (
        SELECT dt, recent_days, order_count AS deliver_suc_count
        FROM (
            SELECT '{date}' AS dt, 1 AS recent_days, SUM(order_count) AS order_count
            FROM dws_trans_org_deliver_suc_1d
            WHERE dt = '{date}'
            UNION ALL
            SELECT '{date}' AS dt, recent_days, SUM(order_count) AS order_count
            FROM dws_trans_org_deliver_suc_nd
            WHERE dt = '{date}'
            GROUP BY recent_days
        ) 
    ) deliver
    FULL OUTER JOIN 
    (
        SELECT dt, recent_days, sort_count
        FROM (
            SELECT '{date}' AS dt, 1 AS recent_days, SUM(sort_count) AS sort_count
            FROM dws_trans_org_sort_1d
            WHERE dt = '{date}'
            UNION ALL
            SELECT '{date}' AS dt, recent_days, SUM(sort_count) AS sort_count
            FROM dws_trans_org_sort_nd
            WHERE dt = '{date}'
            GROUP BY recent_days
        ) 
    ) sort
    ON deliver.dt = sort.dt 
       AND deliver.recent_days = sort.recent_days;
       """
    load_and_verify("ads_express_stats", insert_sql)
    # 5.7 车辆类型维度表：ads_line_stats
    insert_sql = f"""
    insert into ads_line_stats
    select 
        '{date}' as dt,  -- 统一日期参数引用格式
        recent_days,     -- 调整到第二字段
        line_id,
        line_name,
        sum(trans_finish_count) as trans_finish_count,
        sum(trans_finish_distance) as trans_finish_distance,
        sum(trans_finish_dur_sec) as trans_finish_dur_sec,
        sum(trans_finish_order_count) as trans_finish_order_count
    from dws_trans_shift_trans_finish_nd
    where dt = '{date}'  -- 保持日期过滤条件
    -- 调整分组顺序以匹配SELECT字段顺序：
    group by recent_days, line_id, line_name;
    
       """
    load_and_verify("ads_line_stats", insert_sql)

    # 5.8车辆类型维度表：ads_order_cargo_type_stats
    insert_sql = f"""
    INSERT INTO ads_order_cargo_type_stats
    SELECT 
        '{date}' AS dt,
        recent_days,
        cargo_type,
        cargo_type_name,
        SUM(order_count) AS order_count,
        SUM(order_amount) AS order_amount
    FROM (
        -- 1天粒度数据
        SELECT 
            1 AS recent_days,
            cargo_type,
            cargo_type_name,
            order_count,
            order_amount
        FROM dws_trade_org_cargo_type_order_1d
        WHERE dt = '{date}'
        
        UNION ALL
        -- N天粒度数据 (包含7/30天等周期)
        SELECT 
            recent_days,
            cargo_type,
            cargo_type_name,
            order_count,
            order_amount
        FROM dws_trade_org_cargo_type_order_nd
        WHERE dt = '{date}'
    ) combined_data
    GROUP BY recent_days, cargo_type, cargo_type_name;
       """
    load_and_verify("ads_order_cargo_type_stats", insert_sql)

    # 5.9 车辆类型维度表：ads_order_stats
    insert_sql = f"""
     INSERT INTO ads_order_stats
    SELECT 
        '{date}' AS dt,
        recent_days,
        SUM(order_count) AS order_count,
        SUM(order_amount) AS order_amount
    FROM (
        -- 1天粒度数据
        SELECT 
            1 AS recent_days,
            order_count,
            order_amount
        FROM dws_trade_org_cargo_type_order_1d
        WHERE dt = '{date}'
        
        UNION ALL
        
        -- N天粒度数据
        SELECT 
            recent_days,
            order_count,
            order_amount
        FROM dws_trade_org_cargo_type_order_nd
        WHERE dt = '{date}'
    ) combined_data
    GROUP BY recent_days;
       """
    load_and_verify("ads_order_stats", insert_sql)

    # 5.10 车辆类型维度表：ads_org_stats
    insert_sql = f"""
    INSERT INTO TABLE ads_org_stats
SELECT 
    dt,
    recent_days,
    org_id,
    org_name,
    COALESCE(order_count, 0) AS order_count,
    COALESCE(order_amount, 0) AS order_amount,
    COALESCE(trans_finish_count, 0) AS trans_finish_count,
    COALESCE(trans_finish_distance, 0) AS trans_finish_distance,
    COALESCE(trans_finish_dur_sec, 0) AS trans_finish_dur_sec,
    COALESCE(avg_trans_finish_distance, 0) AS avg_trans_finish_distance,
    COALESCE(avg_trans_finish_dur_sec, 0) AS avg_trans_finish_dur_sec
FROM (
    -- 当日增量数据（订单+运输合并）
    SELECT 
        COALESCE(ord.org_id, trn.org_id) AS org_id,
        COALESCE(ord.org_name, trn.org_name) AS org_name,
        '{date}' AS dt,  -- 统一指定分区日期
        1 AS recent_days,  -- 固定当日维度
        COALESCE(ord.order_count, 0) AS order_count,  
        COALESCE(ord.order_amount, 0) AS order_amount,  
        COALESCE(trn.trans_finish_count, 0) AS trans_finish_count, 
        COALESCE(trn.trans_finish_distance, 0) AS trans_finish_distance,
        COALESCE(trn.trans_finish_dur_sec, 0) AS trans_finish_dur_sec,
        trn.avg_trans_finish_distance, 
        trn.avg_trans_finish_dur_sec
    FROM (
        SELECT 
            org_id,
            org_name,
            SUM(order_count) AS order_count,
            SUM(order_amount) AS order_amount
        FROM dws_trade_org_cargo_type_order_1d
        WHERE dt = '{date}'
        GROUP BY org_id, org_name
    ) ord
    FULL OUTER JOIN (  -- 全外连接确保数据完整
        SELECT 
            org_id,
            org_name,
            SUM(trans_finish_count) AS trans_finish_count,
            SUM(trans_finish_distance) AS trans_finish_distance,
            SUM(trans_finish_dur_sec) AS trans_finish_dur_sec,
            CASE WHEN SUM(trans_finish_count) = 0 THEN 0 
                 ELSE SUM(trans_finish_distance)/SUM(trans_finish_count) 
            END AS avg_trans_finish_distance,
            CASE WHEN SUM(trans_finish_count) = 0 THEN 0 
                 ELSE SUM(trans_finish_dur_sec)/SUM(trans_finish_count) 
            END AS avg_trans_finish_dur_sec
        FROM dws_trans_org_truck_model_type_trans_finish_1d
        WHERE dt = '{date}'
        GROUP BY org_id, org_name
    ) trn 
    ON ord.org_id = trn.org_id 
    AND ord.org_name = trn.org_name  

    UNION ALL
    -- 历史维度数据（按多日粒度聚合）
    SELECT 
        COALESCE(ord.org_id, trn.org_id) AS org_id,
        COALESCE(ord.org_name, trn.org_name) AS org_name,
        '{date}' AS dt, 
        COALESCE(ord.recent_days, trn.recent_days) AS recent_days,  
        COALESCE(ord.order_count, 0) AS order_count,
        COALESCE(ord.order_amount, 0) AS order_amount,
        COALESCE(trn.trans_finish_count, 0) AS trans_finish_count,
        COALESCE(trn.trans_finish_distance, 0) AS trans_finish_distance,
        COALESCE(trn.trans_finish_dur_sec, 0) AS trans_finish_dur_sec,
        trn.avg_trans_finish_distance,
        trn.avg_trans_finish_dur_sec
    FROM (
        SELECT 
            recent_days,
            org_id,
            org_name,
            SUM(order_count) AS order_count,
            SUM(order_amount) AS order_amount
        FROM dws_trade_org_cargo_type_order_nd
        WHERE dt = '{date}'
        GROUP BY org_id, org_name, recent_days
    ) ord
    FULL OUTER JOIN (
        SELECT 
            recent_days,
            org_id,
            org_name,
            SUM(trans_finish_count) AS trans_finish_count,
            SUM(trans_finish_distance) AS trans_finish_distance,
            SUM(trans_finish_dur_sec) AS trans_finish_dur_sec,
            CASE WHEN SUM(trans_finish_count) = 0 THEN 0 
                 ELSE SUM(trans_finish_distance)/SUM(trans_finish_count) 
            END AS avg_trans_finish_distance,
            CASE WHEN SUM(trans_finish_count) = 0 THEN 0 
                 ELSE SUM(trans_finish_dur_sec)/SUM(trans_finish_count) 
            END AS avg_trans_finish_dur_sec
        FROM dws_trans_shift_trans_finish_nd
        WHERE dt = '{date}'
        GROUP BY org_id, org_name, recent_days
    ) trn 
    ON ord.org_id = trn.org_id 
    AND ord.org_name = trn.org_name
    AND ord.recent_days = trn.recent_days 
);
       """
    load_and_verify("ads_org_stats", insert_sql)

    # 5.11 车辆类型维度表：ads_shift_stats
    insert_sql = f"""
     INSERT INTO ads_shift_stats
    SELECT 
        '{date}' AS dt, 
        recent_days,
        shift_id,
        SUM(trans_finish_count) AS trans_finish_count,
        SUM(trans_finish_distance) AS trans_finish_distance,
        SUM(trans_finish_dur_sec) AS trans_finish_dur_sec,
        SUM(trans_finish_order_count) AS trans_finish_order_count
    FROM dws_trans_shift_trans_finish_nd
    WHERE dt = '{date}'  
    GROUP BY shift_id, recent_days; 
       """
    load_and_verify("ads_shift_stats", insert_sql)

    # 5.12 车辆类型维度表：ads_trans_order_stats
    insert_sql = f"""
   INSERT INTO ads_trans_order_stats
SELECT 
    '{date}' AS dt,
    recent_days,
    NVL(receive_order_count, 0) AS receive_order_count,
    NVL(receive_order_amount, 0) AS receive_order_amount,
    NVL(dispatch_order_count, 0) AS dispatch_order_count,
    NVL(dispatch_order_amount, 0) AS dispatch_order_amount
FROM (
    SELECT 
        COALESCE(r.recent_days, d.recent_days) AS recent_days,
        r.receive_order_count,
        r.receive_order_amount,
        d.dispatch_order_count,
        d.dispatch_order_amount
    FROM (
        SELECT 
            1 AS recent_days,
            SUM(order_count) AS receive_order_count,
            SUM(order_amount) AS receive_order_amount
        FROM dws_trans_org_receive_1d
        WHERE dt = '{date}'
        
        UNION ALL
        
        SELECT 
            recent_days,
            SUM(order_count) AS receive_order_count,
            SUM(order_amount) AS receive_order_amount
        FROM dws_trans_org_receive_nd
        WHERE dt = '{date}'
        GROUP BY recent_days
    ) r
    FULL OUTER JOIN (
        SELECT 
            1 AS recent_days,
            SUM(order_count) AS dispatch_order_count,
            SUM(order_amount) AS dispatch_order_amount
        FROM dws_trans_dispatch_1d
        WHERE dt = '{date}'
        
        UNION ALL
        
        SELECT 
            recent_days,
            SUM(order_count) AS dispatch_order_count,
            SUM(order_amount) AS dispatch_order_amount
        FROM dws_trans_dispatch_nd
        WHERE dt = '{date}'
        GROUP BY recent_days
    ) d
    ON r.recent_days = d.recent_days
) combined; 

       """
    load_and_verify("ads_trans_order_stats", insert_sql)

    # 5.13 车辆类型维度表：ads_trans_order_stats_td
    insert_sql = f"""
    INSERT INTO ads_trans_order_stats_td
SELECT 
    dt,
    SUM(bounding_order_count) AS bounding_order_count,
    SUM(bounding_order_amount) AS bounding_order_amount
FROM (
    -- 派送订单数据
    SELECT 
        dt,
        order_count AS bounding_order_count,
        order_amount AS bounding_order_amount
    FROM dws_trans_dispatch_td
    WHERE dt = '{date}'
    UNION ALL  
    -- 已完成订单数据（负数）
    SELECT 
        dt,
        order_count * (-1) AS bounding_order_count,
        order_amount * (-1) AS bounding_order_amount
    FROM dws_trans_bound_finish_td
    WHERE dt = '{date}'
) AS combined_data
GROUP BY dt;  

       """
    load_and_verify("ads_trans_order_stats_td", insert_sql)

    # 5.14 车辆类型维度表：ads_trans_stats
    insert_sql = f"""
    INSERT INTO ads_trans_stats
    SELECT '{date}' AS dt, 
           1 AS recent_days,
           SUM(trans_finish_count) AS trans_finish_count,
           SUM(trans_finish_distance) AS trans_finish_distance,
           SUM(trans_finish_dur_sec) AS trans_finish_dur_sec
    FROM dws_trans_org_truck_model_type_trans_finish_1d
    WHERE dt = '{date}'
    UNION ALL
    SELECT '{date}' AS dt, 
           recent_days, 
           SUM(trans_finish_count) AS trans_finish_count,
           SUM(trans_finish_distance) AS trans_finish_distance,
           SUM(trans_finish_dur_sec) AS trans_finish_dur_sec
    FROM dws_trans_shift_trans_finish_nd
    WHERE dt = '{date}'
    GROUP BY recent_days; 
       """
    load_and_verify("ads_trans_stats", insert_sql)

    # 5.15 车辆类型维度表：ads_truck_stats
    insert_sql = f"""
   INSERT INTO ads_order_stats
    SELECT 
        '{date}' AS dt,
        recent_days,
        SUM(order_count) AS order_count,
        SUM(order_amount) AS order_amount
    FROM 
    (
        SELECT 
            1 AS recent_days,
            order_count,
            order_amount
        FROM dws_trade_org_cargo_type_order_1d
        WHERE dt = '{date}'
        
        UNION ALL
        
        SELECT 
            recent_days,
            order_count,
            order_amount
        FROM dws_trade_org_cargo_type_order_nd
        WHERE dt = '{date}'
    ) combined_data
    GROUP BY recent_days;
    """
    load_and_verify("ads_order_stats", insert_sql)

    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()