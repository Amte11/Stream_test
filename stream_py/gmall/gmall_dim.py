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
    # 5.2 快递员维度表：dim_promotion_pos_full
    insert_sql = f"""
    insert into table dim_activity_full partition(`dt`='{date}')
    select
        rule.id,
        info.id,
        activity_name,
        rule.activity_type,
        dic.dic_name,
        activity_desc,
        start_time,
        end_time,
        create_time,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        case rule.activity_type
            when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
            when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
            when '3103' then concat('打', benefit_discount,'折')
        end benefit_rule,
        benefit_level
    from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from ods_activity_rule
        where dt='{date}'
    )rule
    left join
    (
        select
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        from ods_activity_info
        where dt='{date}'
    )info
    on rule.activity_id=info.id
    left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where dt='{date}'
        and parent_code='31'
    )dic
    on rule.activity_type=dic.dic_code;
       """
    load_and_verify("dim_promotion_pos_full", insert_sql)
    # 5.1 园区维度表：dim_coupon_full
    insert_sql = f"""
      insert into table dim_coupon_full partition(`dt`='{date}')
select
    id,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3203' then concat('减',benefit_amount,'元')
    end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
(
    select
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time
    from ods_coupon_info
    where `dt`='{date}'
)ci
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic
    where `dt`='{date}'
    and parent_code='32'
)coupon_dic
on ci.coupon_type=coupon_dic.dic_code
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic
    where `dt`='{date}'
    and parent_code='33'
)range_dic
on ci.range_type=range_dic.dic_code;
    """
    load_and_verify("dim_coupon_full", insert_sql)

    # 5.2 快递员维度表：dim_promotion_pos_full
    insert_sql = f"""
    insert into table dim_promotion_pos_full partition(dt='{date}')
            select
            `id`,
            `pos_location`,
            `pos_type`,
            `promotion_type`,
            `create_time`,
            `operate_time`
        from ods_promotion_pos
        where dt='{date}';
    """
    load_and_verify("dim_promotion_pos_full", insert_sql)
    # 5.3 快递员维度表：dim_promotion_refer_full
    insert_sql = f"""
    insert into table dim_promotion_refer_full partition(dt='{date}')
     select
    `id`,
    `refer_name`,
    `create_time`,
    `operate_time`
from ods_promotion_refer
where dt='{date}';
        """
    load_and_verify("dim_promotion_refer_full", insert_sql)

    # 5.4 机构维度表：dim_province_full（修正父机构名称逻辑）
    insert_sql = f"""
   insert into table dim_province_full partition(dt='{date}')
         select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
(
    select
        id,
        name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods_base_province
    where dt='{date}'
)province
left join
(
    select
        id,
        region_name
    from ods_base_region
    where dt='{date}'
)region
on province.region_id=region.id;"""
    load_and_verify("dim_province_full", insert_sql)

    # 5.5 区域维度表：dim_sku_full（简单直接，保留原逻辑）
    insert_sql = f"""
       with
sku as
(
    select
        id,
        CAST(price AS double) as price,  -- 转 double
        sku_name,
        sku_desc,
        CAST(weight AS float) as weight,  -- 转 float
        CAST(is_sale AS boolean) as is_sale,  -- 转 boolean
        spu_id,
        category3_id,
        tm_id,
        create_time  -- 若 Hive 表是 timestamp，确认 ods_sku_info.create_time 类型
    from ods_sku_info
    where dt='{date}'
),
spu as
(
    select
        id,
        spu_name
    from ods_spu_info
    where dt='{date}'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ods_base_category3
    where dt='{date}'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ods_base_category2
    where dt='{date}'
),
c1 as
(
    select
        id,
        name
    from ods_base_category1
    where dt='{date}'
),
tm as
(
    select
        id,
        tm_name
    from ods_base_trademark
    where dt='{date}'
),
attr as (
    select
        sku_id,
        collect_set(
            named_struct(
                'attr_id', CAST(attr_id AS STRING),
                'value_id', CAST(value_id AS STRING),
                'attr_name', attr_name,
                'value_name', value_name
            )
        ) as attrs
    from ods_sku_attr_value
    where dt='{date}'
    group by sku_id
),
sale_attr as (
    select
        sku_id,
        collect_set(
            named_struct(
                'sale_attr_id', CAST(sale_attr_id AS STRING),  -- 若目标表是 string 就转
                'sale_attr_value_id', CAST(sale_attr_value_id AS STRING),
                'sale_attr_name', sale_attr_name,
                'sale_attr_value_name', sale_attr_value_name
            )
        ) as sale_attrs

    from ods_sku_sale_attr_value
    where dt='{date}'
    group by sku_id
)
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
    """
    load_and_verify("dim_sku_full", insert_sql)

    # 5.6 班次维度表：dim_user_zip（左连接关联字典表）
    insert_sql = f"""
   insert into table dim_user_zip partition (dt = '{date}')
        SELECT 
    data.id,
    -- 姓名脱敏：首字符保留，其余用*（空值兼容）
    CASE 
        WHEN data.name IS NOT NULL AND LENGTH(data.name) > 0 
        THEN CONCAT(SUBSTR(data.name, 1, 1), REPEAT('*', LENGTH(data.name) - 1))
        ELSE data.name 
    END AS name,
    
    -- 手机号脱敏：正则匹配+分段脱敏（空值返回NULL更合理）
    CASE 
        WHEN data.phone_num REGEXP '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$'
        THEN CONCAT(SUBSTR(data.phone_num, 1, 3), '****', SUBSTR(data.phone_num, 8))
        ELSE data.phone_num 
    END AS phone_num,
    
    -- 邮箱脱敏：@前隐藏，保留域名（空值返回NULL更合理）
    CASE 
        WHEN data.email REGEXP '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$'
        THEN CONCAT(REPEAT('*', LENGTH(SPLIT(data.email, '@')[0])), '@', SPLIT(data.email, '@')[1])
        ELSE data.email 
    END AS email,
    
    data.user_level,
    data.birthday,
    data.gender,
    data.create_time,
    data.operate_time,
    '{date}' AS start_date,
    '9999-12-31' AS end_date
FROM ods_user_info data
WHERE dt = '{date}';
    """
    load_and_verify("dim_user_zip", insert_sql)



    # 6. 关闭Spark
    spark.stop()
    print("===== 所有维度表加载完成 =====")


if __name__ == "__main__":
    main()