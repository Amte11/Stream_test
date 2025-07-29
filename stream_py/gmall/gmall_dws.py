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
        date = "2025-07-03"  # 替换为实际业务日期（必须是源表存在的dt分区）
    print(f"===== 执行日期: {date} =====")

    # 3. 切换数据库
    spark.sql("use gmall;")

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
    # 5.1 园区维度表：dws_interaction_sku_favor_add_1d
    insert_sql = f"""
      insert into table dws_interaction_sku_favor_add_1d partition(dt)
      select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    favor_add_count,
    dt
from
(
    select
        dt,
        sku_id,
        count(*) favor_add_count
    from dwd_interaction_favor_add_inc
    group by dt,sku_id
)favor
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt='2025-07-04'
)sku
on favor.sku_id=sku.id;

    """
    load_and_verify("dws_interaction_sku_favor_add_1d", insert_sql)

    # 5.2 快递员维度表：dws_tool_user_coupon_coupon_used_1d
    insert_sql = f"""
    insert into table dws_tool_user_coupon_coupon_used_1d partition(dt)
           select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count,
    dt
from
(
    select
        dt,
        user_id,
        coupon_id,
        count(*) used_count
    from dwd_tool_coupon_used_inc
    group by dt,user_id,coupon_id
)t1
left join
(
    select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where dt='{date}'
)t2
on t1.coupon_id=t2.id;
    """
    load_and_verify("dws_tool_user_coupon_coupon_used_1d", insert_sql)
    # 5.3 快递员维度表：dws_trade_province_order_1d
    insert_sql = f"""
    insert into table dws_trade_province_order_1d partition(dt)
    select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    dt
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d,
        dt
    from dwd_trade_order_detail_inc
    group by province_id,dt
)o
left join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_province_full
    where dt='{date}'
)p
on o.province_id=p.id;
        """
    load_and_verify("dws_trade_province_order_1d", insert_sql)

    # 5.4 机构维度表：dws_trade_province_order_nd（修正父机构名称逻辑）
    insert_sql = f"""
    insert into table dws_trade_province_order_nd partition(dt='{date}')
        select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('{date}',-6),order_count_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where dt>=date_add('{date}',-29)
and dt<='{date}'
group by province_id,province_name,area_code,iso_code,iso_3166_2; """
    load_and_verify("dws_trade_province_order_nd", insert_sql)

    # 5.5 区域维度表：dws_trade_user_cart_add_1d（简单直接，保留原逻辑）
    insert_sql = f"""
    insert into table dws_trade_user_cart_add_1d partition(dt)
      select
        user_id,
        count(*),
        sum(sku_num),
        dt
    from dwd_trade_cart_add_inc
    group by user_id,dt;
    """
    load_and_verify("dws_trade_user_cart_add_1d", insert_sql)

    # 5.6 班次维度表：dws_trade_user_order_1d（左连接关联字典表）
    insert_sql = f"""
    insert into table dws_trade_user_order_1d partition(dt)
     select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount),
    dt
from dwd_trade_order_detail_inc
group by user_id,dt;
    """
    load_and_verify("dws_trade_user_order_1d", insert_sql)

    # 5.7 司机维度表：dws_trade_user_order_td（保留原左连接逻辑）
    insert_sql = f"""
    insert into table dws_trade_user_order_td partition(dt='{date}')
    select
            user_id,
            min(dt) order_date_first,
            max(dt) order_date_last,
            sum(order_count_1d) order_count,
            sum(order_num_1d) order_num,
            sum(order_original_amount_1d) original_amount,
            sum(activity_reduce_amount_1d) activity_reduce_amount,
            sum(coupon_reduce_amount_1d) coupon_reduce_amount,
            sum(order_total_amount_1d) total_amount
        from dws_trade_user_order_1d
        group by user_id;

    """
    load_and_verify("dws_trade_user_order_td", insert_sql)

    # 5.8 车辆维度表：dws_trade_user_payment_1d（左连接关联所有维度）
    insert_sql = f"""
   insert into table dws_trade_user_payment_1d partition(dt)
        select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd_trade_pay_detail_suc_inc
group by user_id,dt;

    """
    load_and_verify("dws_trade_user_payment_1d", insert_sql)

    # 5.9 用户地址维度表：dwd_trans_deliver_suc_detail_inc（时间转换修正）
    insert_sql = f"""
    insert into table dws_trade_user_sku_order_1d partition(dt)
      select

    id,
    user_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count_1d,
    order_num_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    dt
from
(
    select
        dt,
        user_id,
        sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from dwd_trade_order_detail_inc
    group by dt,user_id,sku_id
)od
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt='2025-07-04'
)sku
on od.sku_id=sku.id;

    """
    load_and_verify("dws_trade_user_sku_order_1d", insert_sql)

    # 5.10 用户维度表：dws_trade_user_sku_order_nd（时间戳转换修正）
    insert_sql = f"""
    insert into table dws_trade_user_sku_order_nd partition(dt='{date}')
   select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(dt>=date_add('{date}',-6),order_count_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_num_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('{date}',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt>=date_add('{date}',-29)
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;

    """
    load_and_verify("dws_trade_user_sku_order_nd", insert_sql)

    # 5.11 用户维度表：dws_traffic_page_visitor_page_view_1d（时间戳转换修正）
    insert_sql = f"""
    insert into table dws_traffic_page_visitor_page_view_1d partition(dt='{date}')
      select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where dt='{date}'
group by mid_id,brand,model,operate_system,page_id;


        """
    load_and_verify("dws_traffic_page_visitor_page_view_1d", insert_sql)

    # 5.12 用户维度表：dws_traffic_session_page_view_1d（时间戳转换修正）
    insert_sql = f"""
   insert into table dws_traffic_session_page_view_1d partition(dt='{date}')
   select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where dt='{date}'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;

        """
    load_and_verify("dws_traffic_session_page_view_1d", insert_sql)

    # 5.13 用户维度表：dws_user_user_login_td（时间戳转换修正）
    insert_sql = f"""
    insert into table dws_user_user_login_td partition (dt = '{date}')
     select u.id                                                         user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                       login_count_td
from (
         select id,
                create_time
         from dim_user_zip
         where dt = '{date}'
     ) u
         left join
     (
         select user_id,
                max(dt)  login_date_last,
                count(*) login_count_td
         from dwd_user_login_inc
         group by user_id
     ) l
     on u.id = l.user_id;

            """
    load_and_verify("dws_user_user_login_td", insert_sql)

    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()