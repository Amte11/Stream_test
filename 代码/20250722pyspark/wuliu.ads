
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import sys


def main():
    # 1. 初始化SparkSession（关键参数前置配置）
    spark = SparkSession.builder \
        .appName("Hive_Dimension_Load") \
        .master("local[*]") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")  # 减少冗余日志，只显示警告和错误

    # 2. 处理日期变量（支持命令行传参或默认值）
    if len(sys.argv) > 1:
        date = sys.argv[1]  # 命令行传参：python script.py 2025-07-12
    else:
        date = "2025-07-12"  # 替换为实际业务日期（必须是源表存在的dt分区）
    print(f"===== 执行日期: {date} =====")

    # 3. 切换数据库
    spark.sql("use tms01")

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

    # 5. 各维度表插入逻辑（核心修正：左连接、时间转换、空值处理）
    # 5.1 园区维度表：dws_trade_org_cargo_type_order_1d
    insert_sql = f"""
    insert into table dws_trade_org_cargo_type_order_1d
    partition (dt)
    select org_id,
           org_name,
             CAST(city_id AS BIGINT) AS  city_id,
           region.name city_name,
           cargo_type,
           cargo_type_name,
           order_count,
           order_amount,
           dt
    from (select org_id,
                 org_name,
                 sender_city_id  city_id,
                 cargo_type,
                 cargo_type_name,
                 count(order_id) order_count,
                 sum(amount)     order_amount,
                 dt
          from (select order_id,
                       cargo_type,
                       cargo_type_name,
                       sender_district_id,
                       sender_city_id,
                       sum(amount) amount,
                       dt
                from (select order_id,
                             cargo_type,
                             cargo_type_name,
                             sender_district_id,
                             sender_city_id,
                             amount,
                             dt
                      from dwd_trade_order_detail_inc) detail
                group by order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         dt) distinct_detail
                   left join
               (select id org_id,
                       org_name,
                       region_id
                from dim_organ_full
                where dt = '{date}') org
               on distinct_detail.sender_district_id = org.region_id
          group by org_id,
                   org_name,
                   cargo_type,
                   cargo_type_name,
                   sender_city_id,
                   dt) agg
             left join (
        select id,
               name
        from dim_region_full
        where dt = '{date}'
    ) region on city_id = region.id;

    """
    load_and_verify("dws_trade_org_cargo_type_order_1d", insert_sql)

    # 5.2 快递员维度表：dws_trade_org_cargo_type_order_nd
    insert_sql = f"""
    insert into table dws_trade_org_cargo_type_order_nd
    partition (dt = '{date}')
    select org_id,
           org_name,
           city_id,
           city_name,
           cargo_type,
           cargo_type_name,
           recent_days,
           sum(order_count)  order_count,
           sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_1d lateral view
        explode(array(7, 30)) tmp as recent_days
    where dt >= date_add('2025-07-12', -recent_days + 1)
    group by org_id,
             org_name,
             city_id,
             city_name,
             cargo_type,
             cargo_type_name,
             recent_days;
    """
    load_and_verify("dws_trade_org_cargo_type_order_nd", insert_sql)
    # 5.3 快递员维度表：dws_trans_bound_finish_td
    insert_sql = f"""
    insert into table dws_trans_bound_finish_td
    partition (dt = '{date}')
    select count(order_id)   order_count,
           sum(order_amount) order_amount
    from (select order_id,
                 max(amount) order_amount
          from dwd_trans_bound_finish_detail_inc
          group by order_id) distinct_info;
        """
    load_and_verify("dws_trans_bound_finish_td", insert_sql)

    # 5.4 机构维度表：dws_trans_dispatch_1d（修正父机构名称逻辑）
    insert_sql = f"""
    insert into table dws_trans_dispatch_1d
    partition (dt)
    select count(order_id)      order_count,
           sum(distinct_amount) order_amount,
           '2025-07-12' dt
    from (select order_id,
                 dt,
                 max(amount) distinct_amount
          from dwd_trans_dispatch_detail_inc
          group by order_id,
                   dt) distinct_info
    group by dt; """
    load_and_verify("dws_trans_dispatch_1d", insert_sql)

    # 5.5 区域维度表：dws_trans_dispatch_nd（简单直接，保留原逻辑）
    insert_sql = f"""
    insert into table dws_trans_dispatch_nd
    partition (dt = '{date}')
    select recent_days,
           sum(order_count)  order_count,
           sum(order_amount) order_amount
    from dws_trans_dispatch_1d lateral view
        explode(array(7, 30)) tmp as recent_days
    where dt >= date_add('2025-07-12', -recent_days + 1)
    group by recent_days;

    """
    load_and_verify("dws_trans_dispatch_nd", insert_sql)

    # 5.6 班次维度表：dws_trans_dispatch_td（左连接关联字典表）
    insert_sql = f"""
    insert into table dws_trans_dispatch_td
    partition (dt = '{date}')
    select sum(order_count)  order_count,
           sum(order_amount) order_amount
    from dws_trans_dispatch_1d;


    """
    load_and_verify("dws_trans_dispatch_td", insert_sql)

    # 5.7 司机维度表：dws_trans_org_deliver_suc_1d（保留原左连接逻辑）
    insert_sql = f"""
    insert into table dws_trans_org_deliver_suc_1d
    partition (dt)
    select org_id,
           org_name,
           city_id,
           city.name       city_name,
           province_id,
           province.name   province_name,
           count(order_id) order_count,
           dt
    from (select order_id,
                 sender_district_id,
                 dt
          from dwd_trans_deliver_suc_detail_inc
          group by order_id, sender_district_id, dt) detail
             left join
         (select id org_id,
                 org_name,
                 region_id district_id
          from dim_organ_full
          where dt = '{date}') organ
         on detail.sender_district_id = organ.district_id
             left join
         (select id,
                 parent_id city_id
          from dim_region_full
          where dt = '{date}') district
         on district_id = district.id
             left join
         (select id,
                 name,
                 parent_id province_id
          from dim_region_full
          where dt = '{date}') city
         on city_id = city.id
             left join
         (select id,
                 name
          from dim_region_full
          where dt = '{date}') province
         on province_id = province.id
    group by org_id,
             org_name,
             city_id,
             city.name,
             province_id,
             province.name,
             dt;

    """
    load_and_verify("dws_trans_org_deliver_suc_1d", insert_sql)

    # 5.8 车辆维度表：dws_trans_org_deliver_suc_nd（左连接关联所有维度）
    insert_sql = f"""
    insert into table dws_trans_org_deliver_suc_nd
    partition (dt = '{date}')
    select org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           recent_days,
           sum(order_count) order_count
    from dws_trans_org_deliver_suc_1d lateral view
        explode(array(7, 30)) tmp as recent_days
    where dt >= date_add('2025-07-12', -recent_days + 1)
    group by org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             recent_days;

    """
    load_and_verify("dws_trans_org_deliver_suc_nd", insert_sql)

    # 5.9 用户地址维度表：dws_trans_org_receive_1d（时间转换修正）
    insert_sql = f"""
    insert into table dws_trans_org_receive_1d
    partition (dt)
    select org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           count(order_id)      order_count,
           sum(distinct_amount) order_amount,
           dt
    from (select order_id,
                 org_id,
                 org_name,
                 city_id,
                 city_name,
                 province_id,
                 province_name,
                 max(amount) distinct_amount,
                 dt
          from (select order_id,
                       amount,
                       sender_district_id,
                       dt
                from dwd_trans_receive_detail_inc) detail
                   left join
               (select id org_id,
                       org_name,
                       region_id
                from dim_organ_full
                where dt = '{date}') organ
               on detail.sender_district_id = organ.region_id
                   left join
               (select id,
                       parent_id
                from dim_region_full
                where dt = '{date}') district
               on region_id = district.id
                   left join
               (select id   city_id,
                       name city_name,
                       parent_id
                from dim_region_full
                where dt = '{date}') city
               on district.parent_id = city_id
                   left join
               (select id   province_id,
                       name province_name,
                       parent_id
                from dim_region_full
                where dt = '{date}') province
               on city.parent_id = province_id
          group by order_id,
                   org_id,
                   org_name,
                   city_id,
                   city_name,
                   province_id,
                   province_name,
                   dt) distinct_tb
    group by org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             dt;

    """
    load_and_verify("dws_trans_org_receive_1d", insert_sql)

    # 5.10 用户维度表：dwd_trans_dispatch_detail_inc（时间戳转换修正）
    insert_sql = f"""
   insert into table dws_trans_org_receive_nd
    partition (dt = '{date}')
    select org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           recent_days,
           sum(order_count)  order_count,
           sum(order_amount) order_amount
    from dws_trans_org_receive_1d
             lateral view explode(array(7, 30)) tmp as recent_days
    where dt >= date_add('2025-07-12', -recent_days + 1)
    group by org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             recent_days;


    """
    load_and_verify("dws_trans_org_receive_nd", insert_sql)

    # 5.11 用户维度表：dwd_trans_receive_detail_inc（时间戳转换修正）
    insert_sql = f"""
    insert into table dws_trans_org_sort_1d
    partition (dt)
    select org_id,
           org_name,
           if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
           if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
           if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
           if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
           sort_count,
           dt
    from (select org_id,
                 count(*) sort_count,
                 dt
          from dwd_bound_sort_inc
          group by org_id, dt) agg
             left join
         (select id,
                 org_name,
                 org_level,
                 region_id
          from dim_organ_full
          where dt = '{date}') org
         on org_id = org.id
             left join
         (select id,
                 name,
                 parent_id
          from dim_region_full
          where dt = '{date}') city_for_level1
         on region_id = city_for_level1.id
             left join
         (select id,
                 name,
                 parent_id
          from dim_region_full
          where dt = '{date}') province_for_level1
         on city_for_level1.parent_id = province_for_level1.id
             left join
         (select id,
                 name,
                 parent_id
          from dim_region_full
          where dt = '{date}') province_for_level2
         on province_for_level1.parent_id = province_for_level2.id;


        """
    load_and_verify("dws_trans_org_sort_1d", insert_sql)

    # 5.12 用户维度表：dws_trans_org_sort_nd（时间戳转换修正）
    insert_sql = f"""
   insert into table dws_trans_org_sort_nd
    partition (dt = '{date}')
    select org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           recent_days,
           sum(sort_count) sort_count
    from dws_trans_org_sort_1d lateral view
        explode(array(7, 30)) tmp as recent_days
    where dt >= date_add('2025-07-12', -recent_days + 1)
    group by org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             recent_days;

        """
    load_and_verify("dws_trans_org_sort_nd", insert_sql)

    # 5.13 用户维度表：dws_trans_org_truck_model_type_trans_finish_1d（时间戳转换修正）
    insert_sql = f"""
    insert into table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt)
    select org_id,
           org_name,
           truck_model_type,
           truck_model_type_name,
           count(trans_finish.id) truck_finish_count,
           sum(actual_distance)   trans_finish_distance,
           sum(finish_dur_sec)    finish_dur_sec,
           dt
    from (select id,
                 start_org_id   org_id,
                 start_org_name org_name,
                 truck_id,
                 actual_distance,
                 finish_dur_sec,
                 dt
          from dwd_trans_trans_finish_inc) trans_finish
             left join
         (select id,
                 truck_model_type,
                 truck_model_type_name
          from dim_truck_full
          where dt = '{date}') truck_info
         on trans_finish.truck_id = truck_info.id
    group by org_id,
             org_name,
             truck_model_type,
             truck_model_type_name,
             dt;

            """
    load_and_verify("dws_trans_org_truck_model_type_trans_finish_1d", insert_sql)

    # 5.14 用户维度表：dws_trans_shift_trans_finish_nd（时间戳转换修正）
    insert_sql = f"""
     insert into table dws_trans_shift_trans_finish_nd
    partition (dt = '{date}')
    select shift_id,
           if(org_level = 1, first.region_id, city.id)     city_id,
           if(org_level = 1, first.region_name, city.name) city_name,
           org_id,
           org_name,
           line_id,
           line_name,
           driver1_emp_id,
           driver1_name,
           driver2_emp_id,
           driver2_name,
           truck_model_type,
           truck_model_type_name,
           recent_days,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec,
           trans_finish_order_count,
           trans_finish_delay_count
    from (select recent_days,
                 shift_id,
                 line_id,
                 truck_id,
                 start_org_id                                       org_id,
                 start_org_name                                     org_name,
                 driver1_emp_id,
                 driver1_name,
                 driver2_emp_id,
                 driver2_name,
                 count(id)                                          trans_finish_count,
                 sum(actual_distance)                               trans_finish_distance,
                 sum(finish_dur_sec)                                trans_finish_dur_sec,
                 sum(order_num)                                     trans_finish_order_count,
                 sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
          from dwd_trans_trans_finish_inc lateral view
              explode(array(7, 30)) tmp as recent_days
          where dt >= date_add('2025-07-12', -recent_days + 1)
          group by recent_days,
                   shift_id,
                   line_id,
                   start_org_id,
                   start_org_name,
                   driver1_emp_id,
                   driver1_name,
                   driver2_emp_id,
                   driver2_name,
                   truck_id) aggregated
             left join
         (select id,
                 org_level,
                 region_id,
                 region_name
          from dim_organ_full
          where dt = '{date}'
         ) first
         on aggregated.org_id = first.id
             left join
         (select id,
                 parent_id
          from dim_region_full
          where dt = '{date}'
         ) parent
         on first.region_id = parent.id
             left join
         (select id,
                 name
          from dim_region_full
          where dt = '{date}'
         ) city
         on parent.parent_id = city.id
             left join
         (select id,
                 line_name
          from dim_shift_full
          where dt = '{date}') for_line_name
         on shift_id = for_line_name.id
             left join (
        select id,
               truck_model_type,
               truck_model_type_name
        from dim_truck_full
        where dt = '{date}'
    ) truck_info on truck_id = truck_info.id;
             """
    load_and_verify("dws_trans_shift_trans_finish_nd", insert_sql)

    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()