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
    # 5.1 园区维度表：dim_complex_full
    insert_sql = f"""
        insert into table dwd_bound_inbound_inc
        partition (dt)
    select id,
           order_id,
           org_id,
           inbound_time ,
           inbound_emp_id,
           substr(inbound_time,1,10) dt
    from ods_order_org_bound
    where dt = '{date}';
    """
    load_and_verify("dwd_bound_inbound_inc", insert_sql)

    # 5.2 快递员维度表：dim_express_courier_full
    insert_sql = f"""
    insert into table dwd_bound_outbound_inc
    partition (dt)
    select id,
           order_id,
           org_id,
           outbound_time,
           outbound_emp_id,
           '{date}'dt
    from ods_order_org_bound
    where dt = '{date}'
      and outbound_time is not null;
    """
    load_and_verify("dwd_bound_outbound_inc", insert_sql)
    # 5.3 快递员维度表：dim_express_courier_full
    insert_sql = f"""
           insert into table dwd_bound_sort_inc
        partition (dt)
    select id,
           order_id,
           org_id,
        sort_time,
           sorter_emp_id,
           '{date}' dt
    from ods_order_org_bound
    where dt = '{date}'
      and sort_time is not null;
        """
    load_and_verify("dwd_bound_sort_inc", insert_sql)

    # 5.4 机构维度表：dwd_trade_order_cancel_detail_inc（修正父机构名称逻辑）
    insert_sql = f"""
insert into table dwd_trade_order_cancel_detail_inc
    partition (dt)
    select 
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name as cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        cancel_time,
        order_no,
        status,
        dic_for_status.name as status_name,
        collect_type,
        dic_for_collect_type.name as collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        '{date}' as dt  -- 关键修复：使用传入的日期变量作为分区值
    from 
        (select 
            id,
            order_id,
            cargo_type,
            volume_length,
            volume_width,
            volume_height,
            weight
         from ods_order_cargo
         where dt = '{date}'
           and is_deleted = '0') cargo
    join 
        (select 
            id,
            order_no,
            status,
            collect_type,
            user_id,
            receiver_complex_id,
            receiver_province_id,
            receiver_city_id,
            receiver_district_id,
            concat(substr(receiver_name, 1, 1), '*') as receiver_name,
            sender_complex_id,
            sender_province_id,
            sender_city_id,
            sender_district_id,
            concat(substr(sender_name, 1, 1), '*') as sender_name,
            cargo_num,
            amount,
            estimate_arrive_time,
            distance,
            concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) as cancel_time
         from ods_order_info
         where dt = '{date}'
           and is_deleted = '0'
           and status = '60050') info
    on cargo.order_id = info.id
    left join 
        (select id, name 
         from ods_base_dic 
         where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
    on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    left join 
        (select id, name 
         from ods_base_dic 
         where dt = '{date}' and is_deleted = '0') dic_for_status
    on info.status = cast(dic_for_status.id as string)
    left join 
        (select id, name 
         from ods_base_dic 
         where dt = '{date}' and is_deleted = '0') dic_for_collect_type
    on info.collect_type = cast(dic_for_collect_type.id as string)  -- 确保连接条件正    """
    load_and_verify("dwd_trade_order_cancel_detail_inc", insert_sql)

    # 5.5 区域维度表：dwd_trade_order_detail_inc（简单直接，保留原逻辑）
    insert_sql = f"""
    insert into table dwd_trade_order_detail_inc
    partition (dt)
    select
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name               as cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        order_time,
        order_no,
        status,
        dic_for_status.name                   as status_name,
        collect_type,
        dic_for_collect_type.name             as collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        date_format(to_date(order_time), 'yyyy-MM-dd') as dt  -- 确保日期格式正确
    from (
        select
            id,
            order_id,
            cargo_type,
            volume_length,
            volume_width,
            volume_height,
            weight,
            concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) as order_time
        from ods_order_cargo
        where dt = '{date}'
            and is_deleted = '0'
    ) cargo
    join (
        select
            id,
            order_no,
            status,
            collect_type,
            user_id,
            receiver_complex_id,
            receiver_province_id,
            receiver_city_id,
            receiver_district_id,
            concat(substr(receiver_name, 1, 1), '*') as receiver_name,
            sender_complex_id,
            sender_province_id,
            sender_city_id,
            sender_district_id,
            concat(substr(sender_name, 1, 1), '*') as sender_name,
            cargo_num,
            amount,
             estimate_arrive_time,
            distance
        from ods_order_info
        where dt = '{date}'
            and is_deleted = '0'
    ) info
    on cargo.order_id = info.id
    left join (
        select
            id,
            name
        from ods_base_dic
        where dt = '{date}'
            and is_deleted = '0'
    ) dic_for_cargo_type
    on cargo.cargo_type = dic_for_cargo_type.id  -- 直接使用ID，避免类型转换
    left join (
        select
            id,
            name
        from ods_base_dic
        where dt = '{date}'
            and is_deleted = '0'
    ) dic_for_status
    on info.status = dic_for_status.id  -- 直接使用ID，避免类型转换
    left join (
        select
            id,
            name
        from ods_base_dic
        where dt = '{date}'
            and is_deleted = '0'
    ) dic_for_collect_type
    on info.collect_type = dic_for_collect_type.id;  -- 修正为dic_for_collect_type

    """
    load_and_verify("dwd_trade_order_detail_inc", insert_sql)

    # 5.6 班次维度表：dwd_trade_order_process_inc（左连接关联字典表）
    insert_sql = f"""
    insert into table dwd_trade_order_process_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name               cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           order_time,
           order_no,
           status,
           dic_for_status.name                   status_name,
           collect_type,
           dic_for_collect_type.name              collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name             payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(order_time, 'yyyy-MM-dd') start_date,
           end_date,
           end_date                              dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight,
                 concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*') receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')   sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 if(status = '60050' or
                    status = '60040',
                    concat(substr(update_time, 1, 10)),
                    '9999-12-31')                               end_date
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0') info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);

    """
    load_and_verify("dwd_trade_order_process_inc", insert_sql)

    # 5.7 司机维度表：dwd_trade_pay_suc_detail_inc（保留原左连接逻辑）
    insert_sql = f"""
    insert into table dwd_trade_pay_suc_detail_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name                 cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           payment_time,
           order_no,
           status,
           dic_for_status.name                     status_name,
           collect_type,
           dic_for_collect_type.name               collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name               payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(payment_time, 'yyyy-MM-dd') dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')                                    sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) payment_time
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0'
            and status <> '60010'
            and status <> '60999') info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);

    """
    load_and_verify("dwd_trade_pay_suc_detail_inc", insert_sql)

    # 5.8 车辆维度表：dwd_trans_bound_finish_detail_inc（左连接关联所有维度）
    insert_sql = f"""
   insert into table dwd_trans_bound_finish_detail_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name                 cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           bound_finish_time,
           order_no,
           status,
           dic_for_status.name                     status_name,
           collect_type,
           dic_for_collect_type.name               collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name               payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(bound_finish_time, 'yyyy-MM-dd') dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')                                    sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) bound_finish_time
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0'
            and status <> '60030'
            and status <> '60020'
        ) info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);

    """
    load_and_verify("dwd_trans_bound_finish_detail_inc", insert_sql)

    # 5.9 用户地址维度表：dwd_trans_deliver_suc_detail_inc（时间转换修正）
    insert_sql = f"""
    insert into table dwd_trans_deliver_suc_detail_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name                 cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           deliver_suc_time,
           order_no,
           status,
           dic_for_status.name                     status_name,
           collect_type,
           dic_for_collect_type.name               collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name               payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(deliver_suc_time, 'yyyy-MM-dd') dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')                                    sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) deliver_suc_time
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0'


              and status <> '60030'
              and status <> '60020'
           ) info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);

    """
    load_and_verify("dwd_trans_deliver_suc_detail_inc", insert_sql)

    # 5.10 用户维度表：dwd_trans_dispatch_detail_inc（时间戳转换修正）
    insert_sql = f"""
    insert into table dwd_trans_dispatch_detail_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name                 cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           dispatch_time,
           order_no,
           status,
           dic_for_status.name                     status_name,
           collect_type,
           dic_for_collect_type.name               collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name               payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(dispatch_time, 'yyyy-MM-dd') dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')                                    sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) dispatch_time
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0'
            and status <> '60010'
            and status <> '60020'
            and status <> '60030'
            and status <> '60040'
            and status <> '60999') info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);

    """
    load_and_verify("dwd_trans_dispatch_detail_inc", insert_sql)

    # 5.11 用户维度表：dwd_trans_receive_detail_inc（时间戳转换修正）
    insert_sql = f"""
    insert into table dwd_trans_receive_detail_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name                 cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           receive_time,
           order_no,
           status,
           dic_for_status.name                     status_name,
           collect_type,
           dic_for_collect_type.name               collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name               payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(receive_time, 'yyyy-MM-dd') dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')                                    sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) receive_time
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0'
            and status <> '60010'
            and status <> '60020'
            and status <> '60999') info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);


        """
    load_and_verify("dwd_trans_receive_detail_inc", insert_sql)

    # 5.12 用户维度表：dwd_trans_sign_detail_inc（时间戳转换修正）
    insert_sql = f"""
   insert into table dwd_trans_sign_detail_inc
    partition (dt)
    select cargo.id,
           order_id,
           cargo_type,
           dic_for_cargo_type.name                 cargo_type_name,
           volume_length,
           volume_width,
           volume_height,
           weight,
           sign_time,
           order_no,
           status,
           dic_for_status.name                     status_name,
           collect_type,
           dic_for_collect_type.name               collect_type_name,
           user_id,
           receiver_complex_id,
           receiver_province_id,
           receiver_city_id,
           receiver_district_id,
           receiver_name,
           sender_complex_id,
           sender_province_id,
           sender_city_id,
           sender_district_id,
           sender_name,
           payment_type,
           dic_for_payment_type.name               payment_type_name,
           cargo_num,
           amount,
           estimate_arrive_time,
           distance,
           date_format(sign_time, 'yyyy-MM-dd') dt
    from (select id,
                 order_id,
                 cargo_type,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight
          from ods_order_cargo
          where dt = '{date}'
            and is_deleted = '0') cargo
             join
         (select id,
                 order_no,
                 status,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 concat(substr(sender_name, 1, 1), '*')                                    sender_name,
                 payment_type,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) sign_time
          from ods_order_info
          where dt = '{date}'
            and is_deleted = '0'
              and status <> '60030'
              and status <> '60020'
        ) info
         on cargo.order_id = info.id
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_cargo_type
         on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_status
         on info.status = cast(dic_for_status.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_collect_type
         on info.collect_type = cast(dic_for_cargo_type.id as string)
             left join
         (select id,
                 name
          from ods_base_dic
          where dt = '{date}'
            and is_deleted = '0') dic_for_payment_type
         on info.payment_type = cast(dic_for_payment_type.id as string);

        """
    load_and_verify("dwd_trans_sign_detail_inc", insert_sql)

    # 5.13 用户维度表：dwd_trans_trans_finish_inc（时间戳转换修正）
    insert_sql = f"""
    insert into table dwd_trans_trans_finish_inc
    partition (dt)
    select info.id,
           shift_id,
           line_id,
           start_org_id,
           start_org_name,
           end_org_id,
           end_org_name,
           order_num,
           driver1_emp_id,
           driver1_name,
           driver2_emp_id,
           driver2_name,
           truck_id,
           truck_no,
           actual_start_time,
           actual_end_time,
           estimated_time estimate_end_time,
           actual_distance,
           finish_dur_sec,
          date_format(actual_end_time, 'yyyy-MM-dd') dt
    from (select id,
                 shift_id,
                 line_id,
                 start_org_id,
                 start_org_name,
                 end_org_id,
                 end_org_name,
                 order_num,
                 driver1_emp_id,
                 concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
                 driver2_emp_id,
                 concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
                 truck_id,
                 md5(truck_no)                                                                      truck_no,
                  actual_start_time,
                  actual_end_time,
                 actual_distance,
                 (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec
          from ods_transport_task
          where dt = '{date}'
            and is_deleted = '0'
            and actual_end_time is not null) info
             left join
         (select id,
                 estimated_time
          from dim_shift_full
          where dt = '{date}') dim_tb
         on info.shift_id = dim_tb.id;

            """
    load_and_verify("dwd_trans_trans_finish_inc", insert_sql)
    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()