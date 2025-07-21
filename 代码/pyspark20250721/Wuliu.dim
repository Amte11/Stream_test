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
    sc.setLogLevel("WARN")  # 减少冗余日志，只显示警告和错误  我知道了，我重新下载的后面没

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
    insert into table dim_complex_full partition (dt = '{date}')
    select
        complex_info.id as id,
        complex_info.complex_name,
        complex_courier.courier_emp_ids,
        complex_info.province_id,
        dic_prov.name as province_name,
        complex_info.city_id,
        dic_city.name as city_name,
        complex_info.district_id,
        complex_info.district_name
    from (
        select id, complex_name, province_id, city_id, district_id, district_name
        from ods_base_complex
        where dt = '{date}' and is_deleted = '0'
    ) complex_info
    -- 左连接保留主表数据（原内连接可能过滤全部数据）
    left join (
        select id, name
        from ods_base_region_info
        where dt = '{date}' and is_deleted = '0'
    ) dic_prov on complex_info.province_id = dic_prov.id
    left join (
        select id, name
        from ods_base_region_info
        where dt = '{date}' and is_deleted = '0'
    ) dic_city on complex_info.city_id = dic_city.id
    left join (
        select complex_id, collect_set(cast(courier_emp_id as string)) as courier_emp_ids
        from ods_express_courier_complex
        where dt = '{date}' and is_deleted = '0'
        group by complex_id
    ) complex_courier on complex_info.id = complex_courier.complex_id;
    """
    load_and_verify("dim_complex_full", insert_sql)


    # 5.2 快递员维度表：dim_express_courier_full
    insert_sql = f"""
    insert into table dim_express_courier_full partition (dt = '{date}')
    select
        express_cor_info.id,
        emp_id,
        org_id,
        org_name,
        working_phone,
        express_type,
        dic_info.name express_type_name
    from (
        select id, emp_id, org_id, md5(working_phone) working_phone, express_type
        from ods_express_courier
        where dt = '{date}' and is_deleted = '0'
    ) express_cor_info
    -- 左连接避免主表数据丢失
    left join (
        select id, org_name
        from ods_base_organ
        where dt = '{date}' and is_deleted = '0'
    ) organ_info on express_cor_info.org_id = organ_info.id
    left join (
        select id, name
        from ods_base_dic
        where dt = '{date}' and is_deleted = '0'
    ) dic_info on express_cor_info.express_type = dic_info.id;
    """
    load_and_verify("dim_express_courier_full", insert_sql)


    # 5.3 机构维度表：dim_organ_full（修正父机构名称逻辑）
    insert_sql = f"""
    insert into table dim_organ_full partition (dt = '{date}')
    select
        organ_info.id,
        organ_info.org_name,
        org_level,
        region_id,
        region_info.name as region_name,
        region_info.dict_code as region_code,
        org_parent_id,
        -- 修正：关联 ods_base_organ 获取父机构名称
        org_parent.org_name as org_parent_name
    from (
        select id, org_name, org_level, region_id, org_parent_id
        from ods_base_organ
        where dt = '{date}' and is_deleted = '0'
    ) organ_info
    left join (
        select id, name, dict_code
        from ods_base_region_info
        where dt = '{date}' and is_deleted = '0'
    ) region_info on organ_info.region_id = region_info.id
    left join (
        -- 修正：关联 ods_base_organ 获取父机构信息
        select id, org_name
        from ods_base_organ
        where dt = '{date}' and is_deleted = '0'
    ) org_parent on organ_info.org_parent_id = org_parent.id;
    """
    load_and_verify("dim_organ_full", insert_sql)


    # 5.4 区域维度表：dim_region_full（简单直接，保留原逻辑）
    insert_sql = f"""
    insert into table dim_region_full partition (dt = '{date}')
    select id, parent_id, name, dict_code, short_name
    from ods_base_region_info
    where dt = '{date}' and is_deleted = '0';
    """
    load_and_verify("dim_region_full", insert_sql)


    # 5.5 班次维度表：dim_shift_full（左连接关联字典表）
    insert_sql = f"""
    insert into table dim_shift_full partition (dt = '{date}')
    select
        shift_info.id,
        line_id,
        line_info.name as line_name,
        line_no,
        line_level,
        org_id,
        transport_line_type_id,
        dic_info.name as transport_line_type_name,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        pair_line_id,
        distance,
        cost,
        estimated_time,
        start_time,
        driver1_emp_id,
        driver2_emp_id,
        truck_id,
        pair_shift_id
    from (
        select id, line_id, start_time, driver1_emp_id, driver2_emp_id, truck_id, pair_shift_id
        from ods_line_base_shift
        where dt = '{date}' and is_deleted = '0'
    ) shift_info
    left join (
        select id, name, line_no, line_level, org_id, transport_line_type_id,
               start_org_id, start_org_name, end_org_id, end_org_name,
               pair_line_id, distance, cost, estimated_time
        from ods_line_base_info
        where dt = '{date}' and is_deleted = '0'
    ) line_info on shift_info.line_id = line_info.id
    left join (
        select id, name
        from ods_base_dic
        where dt = '{date}' and is_deleted = '0'
    ) dic_info on line_info.transport_line_type_id = dic_info.id;
    """
    load_and_verify("dim_shift_full", insert_sql)


    # 5.6 司机维度表：dim_truck_driver_full（保留原左连接逻辑）
    insert_sql = f"""
    insert into table dim_truck_driver_full partition (dt = '{date}')
    select
        driver_info.id,
        emp_id,
        driver_info.org_id,
        organ_info.org_name,
        driver_info.team_id,
        team_info.name as team_name,
        license_type,
        coalesce(init_license_date, '1970-01-01 00:00:00') as init_license_date,
        coalesce(expire_date, '2099-12-31 23:59:59') as expire_date,
        license_no,
        is_enabled
    from (
        select id, emp_id, org_id, team_id, license_type, init_license_date,
               expire_date, license_no, is_enabled
        from ods_truck_driver
        where dt = '{date}' and is_deleted = '0'
    ) driver_info
    left join (
        select id, org_name
        from ods_base_organ
        where dt = '{date}' and is_deleted = '0'
    ) organ_info on driver_info.org_id = organ_info.id
    left join (
        select id, name
        from ods_truck_team
        where dt = '{date}' and is_deleted = '0'
    ) team_info on driver_info.team_id = team_info.id;
    """
    load_and_verify("dim_truck_driver_full", insert_sql)


    # 5.7 车辆维度表：dim_truck_full（左连接关联所有维度）
    insert_sql = f"""
    insert into table dim_truck_full partition (dt = '{date}')
    select
        truck_info.id,
        team_id,
        team_info.name as team_name,
        team_no,
        org_id,
        org_name,
        manager_emp_id,
        truck_no,
        truck_model_id,
        model_name as truck_model_name,
        model_type as truck_model_type,
        dic_for_type.name as truck_model_type_name,
        model_no as truck_model_no,
        brand as truck_brand,
        dic_for_brand.name as truck_brand_name,
        truck_weight,
        load_weight,
        total_weight,
        eev,
        boxcar_len,
        boxcar_wd,
        boxcar_hg,
        max_speed,
        oil_vol,
        device_gps_id,
        engine_no,
        license_registration_date,
        license_last_check_date,
        license_expire_date,
        is_enabled
    from (
        select id, team_id, md5(truck_no) as truck_no, truck_model_id,
               device_gps_id, engine_no, license_registration_date,
               license_last_check_date, license_expire_date, is_enabled
        from ods_truck_info
        where dt = '{date}' and is_deleted = '0'
    ) truck_info
    left join (
        select id, name, team_no, org_id, manager_emp_id
        from ods_truck_team
        where dt = '{date}' and is_deleted = '0'
    ) team_info on truck_info.team_id = team_info.id
    left join (
        select id, model_name, model_type, model_no, brand, truck_weight,
               load_weight, total_weight, eev, boxcar_len, boxcar_wd,
               boxcar_hg, max_speed, oil_vol
        from ods_truck_model
        where dt = '{date}' and is_deleted = '0'
    ) model_info on truck_info.truck_model_id = model_info.id
    left join (
        select id, org_name
        from ods_base_organ
        where dt = '{date}' and is_deleted = '0'
    ) organ_info on team_info.org_id = organ_info.id
    left join (
        select id, name
        from ods_base_dic
        where dt = '{date}' and is_deleted = '0'
    ) dic_for_type on model_info.model_type = dic_for_type.id
    left join (
        select id, name
        from ods_base_dic
        where dt = '{date}' and is_deleted = '0'
    ) dic_for_brand on model_info.brand = dic_for_brand.id;
    """
    load_and_verify("dim_truck_full", insert_sql)


    # 5.8 用户地址维度表：dim_user_address_zip（时间转换修正）
    insert_sql = f"""
    insert into table dim_user_address_zip partition (dt = '{date}')
    select
        after.id,
        after.user_id,
        after.phone,
        after.province_id,
        after.city_id,
        after.district_id,
        after.complex_id,
        after.address,
        after.is_default,
        -- 修正时间拼接逻辑（处理不同格式的create_time）
        case
            when length(after.create_time) = 19 then after.create_time  -- 已为yyyy-MM-dd HH:mm:ss
            else concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8))
        end as start_date,
        '9999-12-31' as end_date
    from ods_user_address after
    where dt = '{date}' and is_deleted = '0';
    """
    load_and_verify("dim_user_address_zip", insert_sql)


    # 5.9 用户维度表：dim_user_zip（时间戳转换修正）
    insert_sql = f"""
    insert into table dim_user_zip partition (dt = '{date}')
    select
        after.id,
        after.login_name,
        after.nick_name,
        after.passwd,
        after.real_name,
        after.phone_num,
        after.email,
        after.user_level,
        date_add('1970-01-01', cast(after.birthday as int)) as birthday,
        after.gender,
        -- 时间戳转换（兼容秒/毫秒级，处理空值）
        date_format(
            from_utc_timestamp(
                case
                    when length(cast(after.create_time as string)) > 10
                    then timestamp_millis(cast(after.create_time as bigint))  -- 毫秒级
                    else timestamp_seconds(cast(after.create_time as bigint))  -- 秒级
                end,
                'UTC'
            ),
            'yyyy-MM-dd'
        ) as start_date,
        '{date}' as end_date
    from ods_user_info after
    where dt = '{date}' and is_deleted = '0';
    """
    load_and_verify("dim_user_zip", insert_sql)


    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()