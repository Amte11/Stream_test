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
    # 5.1 园区维度表：dwd_interaction_favor_add_inc
    insert_sql = f"""
     insert into table dwd_interaction_favor_add_inc partition(dt)
  select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    date_format(data.create_time,'yyyy-MM-dd')
from ods_favor_info data
where dt='{date}';
    """
    load_and_verify("dwd_interaction_favor_add_inc", insert_sql)

    # 5.2 快递员维度表：dwd_tool_coupon_used_inc
    insert_sql = f"""
    insert into table dwd_tool_coupon_used_inc partition(dt)
     select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time,
    date_format(data.used_time,'yyyy-MM-dd')
from ods_coupon_use data
where dt='{date}'
and data.used_time is not null;
    """
    load_and_verify("dwd_tool_coupon_used_inc", insert_sql)
    # 5.3 快递员维度表：dwd_trade_cart_add_inc
    insert_sql = f"""
   insert into table dwd_trade_cart_add_inc partition (dt)
    select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    data.sku_num,
    date_format(data.create_time, 'yyyy-MM-dd')
from ods_cart_info data
    where dt = '{date}';
        """
    load_and_verify("dwd_trade_cart_add_inc", insert_sql)

    # 5.4 机构维度表：dwd_trade_cart_full（修正父机构名称逻辑）
    insert_sql = f"""
    INSERT into TABLE dwd_trade_order_detail_inc PARTITION (dt)
SELECT
    od.id,
    od.order_id,
    oi.user_id,
    od.sku_id,
    oi.province_id,
    act.activity_id,
    act.activity_rule_id,
    cou.coupon_id,
    DATE_FORMAT(od.create_time, 'yyyy-MM-dd') AS date_id,  -- 保留业务日期字段
    pay.callback_time,
    od.sku_num,
    od.split_original_amount,
    COALESCE(od.split_activity_amount, 0.0) AS split_activity_amount,
    COALESCE(od.split_coupon_amount, 0.0) AS split_coupon_amount,
    od.split_total_amount,  -- 还原原始字段名，避免歧义
    DATE_FORMAT(COALESCE(pay.callback_time, od.create_time), 'yyyy-MM-dd') AS dt  -- 动态分区字段
FROM (
    SELECT
        id,
        order_id,
        sku_id,
        create_time,
        sku_num,
        sku_num * order_price AS split_original_amount,
        split_total_amount,  -- 总金额字段保留原名
        split_activity_amount,
        split_coupon_amount
    FROM ods_order_detail
    WHERE dt = '{date}'
) od
LEFT JOIN (  -- 订单主表（用户/省份）
    SELECT 
        id, 
        user_id, 
        province_id 
    FROM ods_order_info 
    WHERE dt = '{date}'
) oi ON od.order_id = oi.id
LEFT JOIN (  -- 订单活动
    SELECT 
        order_detail_id, 
        activity_id, 
        activity_rule_id 
    FROM ods_order_detail_activity 
    WHERE dt = '{date}'
) act ON od.id = act.order_detail_id
LEFT JOIN (  -- 订单优惠券
    SELECT 
        order_detail_id, 
        coupon_id 
    FROM ods_order_detail_coupon 
    WHERE dt = '{date}'
) cou ON od.id = cou.order_detail_id
LEFT JOIN (  -- 支付信息（关键修复点）
    SELECT 
        order_id, 
        payment_type, 
        dic_name AS payment_name,  -- 直接关联字典表获取名称
        callback_time 
    FROM ods_payment_info pay
    LEFT JOIN (  -- 支付类型字典
        SELECT dic_code, dic_name 
        FROM ods_base_dic 
        WHERE dt = '{date}' AND parent_code = '11'
    ) dic ON pay.payment_type = dic.dic_code
    WHERE pay.dt = '{date}'
) pay ON od.order_id = pay.order_id;
    """
    load_and_verify("dwd_trade_order_detail_inc", insert_sql)

    # 5.6 班次维度表：dwd_trade_pay_detail_suc_inc（左连接关联字典表）
    insert_sql = f"""
    INSERT INTO TABLE dwd_trade_order_detail_inc PARTITION (dt)
    SELECT
        od.id,
        od.order_id,
        oi.user_id,
        od.sku_id,
        oi.province_id,
        act.activity_id,
        act.activity_rule_id,
        cou.coupon_id,
        DATE_FORMAT(od.create_time, 'yyyy-MM-dd') AS date_id,  -- 下单日期
        od.create_time,                                       -- 下单时间
        od.sku_num,
        od.split_original_amount,
        COALESCE(od.split_activity_amount, 0.0) AS split_activity_amount,
        COALESCE(od.split_coupon_amount, 0.0) AS split_coupon_amount,
        od.split_total_amount,                                -- 目标表字段名保持一致
        DATE_FORMAT(od.create_time, 'yyyy-MM-dd') AS dt       -- 分区字段（末尾）
    FROM (
        SELECT
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price AS split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        FROM ods_order_detail
        WHERE dt = '{date}'
    ) od
    LEFT JOIN (
        SELECT id, user_id, province_id 
        FROM ods_order_info 
        WHERE dt = '{date}'
    ) oi ON od.order_id = oi.id
    LEFT JOIN (
        SELECT order_detail_id, activity_id, activity_rule_id 
        FROM ods_order_detail_activity 
        WHERE dt = '{date}'
    ) act ON od.id = act.order_detail_id
    LEFT JOIN (
        SELECT order_detail_id, coupon_id 
        FROM ods_order_detail_coupon 
        WHERE dt = '{date}'
    ) cou ON od.id = cou.order_detail_id;
    """
    load_and_verify("dwd_trade_pay_detail_suc_inc", insert_sql)

    # 5.7 司机维度表：dwd_trade_trade_flow_acc（保留原左连接逻辑）
    insert_sql = f"""
   insert into table dwd_trade_trade_flow_acc partition(dt)
  select
    cast(oi.id as string),  -- 转换为STRING类型
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    cast(create_time as string),  -- 转换为STRING类型
    date_format(callback_time,'yyyy-MM-dd'),
    cast(callback_time as string),  -- 转换为STRING类型
    date_format(finish_time,'yyyy-MM-dd'),
    cast(finish_time as string),  -- 转换为STRING类型
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'9999-12-31') as dt  -- 添加别名
from
(
    select
        data.id,
        data.user_id,
        data.province_id,
        data.create_time,
        data.original_total_amount,
        data.activity_reduce_amount,
        data.coupon_reduce_amount,
        data.total_amount
    from ods_order_info data
    where dt='{date}'
)oi
left join
(
    select
        data.order_id,
        data.callback_time,
        data.total_amount payment_amount
    from ods_payment_info data
    where dt='{date}'
    and data.payment_status='1602'
)pi
on cast(oi.id as string)=cast(pi.order_id as string)  -- 确保连接条件类型一致
left join
(
    select
        data.order_id,
        data.create_time finish_time
    from ods_order_status_log data
    where dt='{date}'
    and data.order_status='1004'
)log
on cast(oi.id as string)=cast(log.order_id as string);  -- 确保连接条件类型一致

    """
    load_and_verify("dwd_trade_trade_flow_acc", insert_sql)

    # 5.8 车辆维度表：dwd_traffic_page_view_inc（左连接关联所有维度）
    insert_sql = f"""
   insert into table dwd_traffic_page_view_inc partition (dt='{date}')
    select
            common.province_id,
            common.brand,
            common.channel,
            common.is_new,
            common.model,
            common.mid_id,
            common.operate_system,
            common.user_id,
            common.version_code,
            page_data.item as page_item,
            page_data.item_type as page_item_type,
            page_data.last_page_id,
            page_data.page_id,
            page_data.from_pos_id,
            page_data.from_pos_seq,
            page_data.refer_id,
            date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
            date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
            common.session_id,
            cast(page_data.during_time as bigint) as during_time  -- 转换为 bigint 类型
        from (
            select
                get_json_object(log, '$.common') as common_json,
                get_json_object(log, '$.page') as page_json,
                get_json_object(log, '$.ts') as ts
            from ods_z_log
            where dt='{date}'
        ) base
        lateral view json_tuple(common_json, 'ar', 'ba', 'ch', 'is_new', 'md', 'mid', 'os', 'uid', 'vc', 'sid') common as province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, session_id
        lateral view json_tuple(page_json, 'item', 'item_type', 'last_page_id', 'page_id', 'from_pos_id', 'from_pos_seq', 'refer_id', 'during_time') page_data as item, item_type, last_page_id, page_id, from_pos_id, from_pos_seq, refer_id, during_time
        where page_json is not null;

    """
    load_and_verify("dwd_traffic_page_view_inc", insert_sql)

    # 5.9 用户地址维度表：dwd_user_login_inc（时间转换修正）
    insert_sql = f"""
    insert into table dwd_user_login_inc partition (dt = '{date}')
    select 
            user_id,
            -- 用 to_timestamp 将数值转为 TIMESTAMP 类型，适配 from_utc_timestamp 参数要求
            date_format(from_utc_timestamp(to_timestamp(CAST(ts AS BIGINT) / 1000), 'GMT+8'), 'yyyy-MM-dd') as date_id,
            date_format(from_utc_timestamp(to_timestamp(CAST(ts AS BIGINT) / 1000), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time,
            channel,
            province_id,
            version_code,
            mid_id,
            brand,
            model,
            operate_system
        from (
            select 
                common_uid as user_id,
                common_ch as channel,
                common_ar as province_id,
                common_vc as version_code,
                common_mid as mid_id,
                common_ba as brand,
                common_md as model,
                common_os as operate_system,
                ts,
                row_number() over (partition by common_sid order by ts) as rn
            from (
                select 
                    get_json_object(log, '$.common.uid') as common_uid,  
                    get_json_object(log, '$.common.ch') as common_ch,
                    get_json_object(log, '$.common.ar') as common_ar,
                    get_json_object(log, '$.common.vc') as common_vc,
                    get_json_object(log, '$.common.mid') as common_mid,
                    get_json_object(log, '$.common.ba') as common_ba,
                    get_json_object(log, '$.common.md') as common_md,
                    get_json_object(log, '$.common.os') as common_os,
                    get_json_object(log, '$.ts') as ts,
                    get_json_object(log, '$.common.sid') as common_sid,
                    get_json_object(log, '$.page') as page
                from ods_z_log
                where dt='{date}'
                  and get_json_object(log, '$.page') is not null  
                  and get_json_object(log, '$.common.uid') is not null  
            ) t1
        ) 
        
        where rn = 1;

    """
    load_and_verify("dwd_user_login_inc", insert_sql)

    # 5.10 用户维度表：dwd_user_register_inc（时间戳转换修正）
    insert_sql = f"""
   insert into table dwd_user_register_inc partition(dt)
       select
    ui.user_id,
    date_format(ui.create_time, 'yyyy-MM-dd') as date_id,
    ui.create_time,
    log.channel,
    log.province_id,
    log.version_code,
    log.mid_id,
    log.brand,
    log.model,
    log.operate_system,
    date_format(ui.create_time, 'yyyy-MM-dd') as dt  -- 确保分区格式与odt一致
from
(
    select
        data.id as user_id,
        data.create_time
    from ods_user_info data
    where dt='{date}'  -- 源表分区格式假设为'yyyy-MM-dd'
    and data.create_time is not null  -- 防御空值
) ui
left join
(
    select
        get_json_object(log, '$.common.ar') as province_id,  -- 从JSON中提取
        get_json_object(log, '$.common.ba') as brand,
        get_json_object(log, '$.common.ch') as channel,
        get_json_object(log, '$.common.md') as model,
        get_json_object(log, '$.common.mid') as mid_id,
        get_json_object(log, '$.common.os') as operate_system,
        get_json_object(log, '$.common.uid') as user_id,
        get_json_object(log, '$.common.vc') as version_code
    from ods_z_log
    where dt='{date}'
    and get_json_object(log, '$.page.page_id') = 'register'  -- 修正JSON路径引用
    and get_json_object(log, '$.common.uid') is not null
) log
on ui.user_id = log.user_id;

    """
    load_and_verify("dwd_user_register_inc", insert_sql)
    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()