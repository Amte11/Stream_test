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

        # 5.13 用户维度表：ads_user_stats（时间戳转换修正）

    insert_sql = f"""
           insert into table ads_user_stats
            select '{date}' dt,
                  recent_days,
                  sum(if(login_date_first >= date_add('{date}', -recent_days + 1), 1, 0)) new_user_count,
                  count(*) active_user_count
           from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
           where dt = '{date}'
             and login_date_last >= date_add('2025-07-04', -recent_days + 1)
           group by recent_days;
                   """
    load_and_verify("ads_user_stats", insert_sql)
    # 5.1 园区维度表：ads_coupon_stats
    insert_sql = f"""
      insert into table ads_coupon_stats
   select
            '{date}' dt,
            coupon_id,
            coupon_name,
            cast(sum(used_count_1d) as bigint),
            cast(count(*) as bigint)
        from dws_tool_user_coupon_coupon_used_1d
        where dt='{date}'
        group by coupon_id,coupon_name;
    """
    load_and_verify("ads_coupon_stats", insert_sql)

    # 5.2 快递员维度表：ads_new_order_user_stats
    insert_sql = f"""
    insert into table ads_new_order_user_stats
            select
            '{date}' dt,
            recent_days,
            count(*) new_order_user_count
        from dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
        where dt='{date}'
        and order_date_first>=date_add('{date}',-recent_days+1)
        group by recent_days;
    """
    load_and_verify("ads_new_order_user_stats", insert_sql)
    # 5.3 快递员维度表：ads_order_by_province
    insert_sql = f"""
           insert into table ads_order_by_province
      select
    '{date}' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws_trade_province_order_1d
where dt='{date}'
union
select
    '{date}' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='{date}';
        """
    load_and_verify("ads_order_by_province", insert_sql)

    # 5.4 机构维度表：ads_order_continuously_user_count（修正父机构名称逻辑）
    insert_sql = f"""
    insert into table ads_order_continuously_user_count
        SELECT
        '{date}' AS dt,
        7 AS metric_type,
        COUNT(DISTINCT user_id) AS count_val
    FROM (
        SELECT
            user_id,
            DATEDIFF(
                LEAD(dt, 2, '9999-12-31') OVER (PARTITION BY user_id ORDER BY dt), 
                dt
            ) AS diff
        FROM dws_trade_user_order_1d
        WHERE dt >= DATE_ADD('{date}', -6)
    ) t1
    WHERE diff = 2  """
    load_and_verify("ads_order_continuously_user_count", insert_sql)

    # 5.5 区域维度表：ads_order_stats_by_cate（简单直接，保留原逻辑）
    insert_sql = f"""
    insert into table ads_order_stats_by_cate
     select
    '{date}' dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='{date}'
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='{date}'
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
)odr;
    """
    load_and_verify("ads_order_stats_by_cate", insert_sql)

    # 5.6 班次维度表：ads_order_stats_by_tm（左连接关联字典表）
    insert_sql = f"""
    insert into table ads_order_stats_by_tm
     select
            '{date}' dt,
            30 ,
            tm_id,
            tm_name,
            order_count,
            order_user_count
        from
        (
            -- 第一段子查询：修正 recent_days 来源（固定为 30）
            select
                 30 as recent_days,  -- 直接赋值固定值
                tm_id,
                tm_name,
                sum(order_count_1d) order_count,
                count(distinct(user_id)) order_user_count
            from dws_trade_user_sku_order_1d
            where dt='{date}'
            group by tm_id, tm_name  -- 按实际字段分组（无 recent_days，因已固定）
        
            union all
        
            -- 第二段子查询（保留原逻辑，recent_days 由 explode 生成）
            select
                recent_days,
                tm_id,
                tm_name,
                sum(order_count),
                count(distinct(if(order_count>0,user_id,null)))
            from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                    end order_count
                from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='{date}'
            )t1
            group by recent_days,tm_id,tm_name
        )odr;
    """
    load_and_verify("ads_order_stats_by_tm", insert_sql)

    # 5.7 司机维度表：dwd_trade_pay_suc_detail_inc（保留原左连接逻辑）
    insert_sql = f"""
  INSERT INTO TABLE ads_order_to_pay_interval_avg
    select
    cast(avg(unix_timestamp(payment_time) - unix_timestamp(order_time)) as string) as order_to_pay_interval_avg,
    '2025-07-03' as dt
    from dwd_trade_trade_flow_acc
    WHERE dt = '{date}'      -- 仅用分区字段过滤
      AND payment_time IS NOT NULL;     -- 排除未支付订单

    """
    load_and_verify("ads_order_to_pay_interval_avg", insert_sql)

    # 5.8 车辆维度表：ads_page_path（左连接关联所有维度）
    insert_sql = f"""
   insert into table ads_page_path
      select
    '{date}' dt,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from dwd_traffic_page_view_inc
        where dt='{date}'
    )t1
)t2
group by source,target;

    """
    load_and_verify("ads_page_path", insert_sql)


    # 5.10 用户维度表：ads_repeat_purchase_by_tm（时间戳转换修正）
    insert_sql = f"""
    insert into table ads_repeat_purchase_by_tm
     select
    '{date}' as dt,
    30 as recent_days,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(order_count_30d) order_count
    from dws_trade_user_sku_order_nd
    where dt='{date}'
    group by user_id, tm_id,tm_name
)t1
group by tm_id,tm_name;

    """
    load_and_verify("ads_repeat_purchase_by_tm", insert_sql)

    # 5.11 用户维度表：ads_sku_cart_num_top3_by_cate（时间戳转换修正）
    insert_sql = f"""
    insert into table ads_sku_cart_num_top3_by_cate
         select
    '{date}' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd_trade_cart_full
        where dt='{date}'
        group by sku_id
    )cart
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
            category3_name
        from dim_sku_full
        where dt='2025-07-04'
    )sku
    on cart.sku_id=sku.id
)t1
where rk<=3;


        """
    load_and_verify("ads_sku_cart_num_top3_by_cate", insert_sql)

    # 5.12 用户维度表：ads_sku_favor_count_top3_by_tm（时间戳转换修正）
    insert_sql = f"""
   INSERT into TABLE ads_sku_favor_count_top3_by_tm 
    SELECT
     '{date}' dt,
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rk
    FROM (
        SELECT
            tm_id,
            tm_name,
            sku_id,
            sku_name,
            favor_add_count_1d,
            rank() OVER (PARTITION BY tm_id ORDER BY favor_add_count_1d DESC) AS rk
        FROM dws_interaction_sku_favor_add_1d
        WHERE dt = '{date}'
    ) t1
    WHERE rk <= 3;

        """
    load_and_verify("ads_sku_favor_count_top3_by_tm", insert_sql)

    # 5.13 用户维度表：ads_traffic_stats_by_channel（时间戳转换修正）
    insert_sql = f"""
    insert into table ads_traffic_stats_by_channel
        select
    '{date}' dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('{date}',-recent_days+1)
group by recent_days,channel;

            """
    load_and_verify("ads_traffic_stats_by_channel", insert_sql)

    # 5.13 用户维度表：ads_user_action（时间戳转换修正）
    insert_sql = f"""
        insert into table ads_user_action
             select
    '{date}' dt,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws_traffic_page_visitor_page_view_1d
    where dt='{date}'
    and page_id in ('home','good_detail')
)page
join
(
    select
        1 recent_days,
        count(*) cart_count
    from dws_trade_user_cart_add_1d
    where dt='{date}'
)cart
on page.recent_days=cart.recent_days
join
(
    select
        1 recent_days,
        count(*) order_count
    from dws_trade_user_order_1d
    where dt='{date}'
)ord
on page.recent_days=ord.recent_days
join
(
    select
        1 recent_days,
        count(*) payment_count
    from dws_trade_user_payment_1d
    where dt='{date}'
)pay
on page.recent_days=pay.recent_days;
                """
    load_and_verify("ads_user_action", insert_sql)

    # 5.13 用户维度表：ads_user_change（时间戳转换修正）
    insert_sql = f"""
        insert into table ads_user_change
          select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '{date}' dt,
        count(*) user_churn_count
    from dws_user_user_login_td
    where dt='{date}'
    and login_date_last=date_add('{date}',-7)
)churn
join
(
    select
        '{date}' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where dt='{date}'
        and login_date_last = '{date}'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where dt=date_add('{date}',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.dt=back.dt;

                """
    load_and_verify("ads_user_change", insert_sql)

    # 5.13 用户维度表：ads_user_retention（时间戳转换修正）
    insert_sql = f"""
        insert into table ads_user_retention
        select '{date}' dt,
       login_date_first create_date,
       datediff('2025-07-04', login_date_first) retention_day,
       sum(if(login_date_last = '2025-07-04', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '2025-07-04', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where dt = '{date}'
           and login_date_first >= date_add('2025-07-04', -7)
           and login_date_first < '2025-07-04'
     ) t1
group by login_date_first;
                """
    load_and_verify("ads_user_retention", insert_sql)



    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()