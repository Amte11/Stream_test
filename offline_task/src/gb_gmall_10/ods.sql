CREATE DATABASE IF NOT EXISTS gmall_10;
USE gmall_10;

--1. 页面访问基础表（ods_page_visit_base）
CREATE TABLE IF NOT EXISTS ods_page_visit_base (
                                                   page_id STRING COMMENT '页面唯一标识',
                                                   page_name STRING COMMENT '页面名称（如首页、618活动承接页）',
                                                   page_type STRING COMMENT '页面类型：home（首页）、custom（自定义承接页）、product_detail（商品详情页）',
                                                   visit_time STRING COMMENT '访问时间，格式：yyyy-MM-dd HH:mm:ss',
                                                   visitor_count BIGINT COMMENT '访客数（暂未统计直播间、短视频等来源，参考文档说明）',
                                                   visit_pv BIGINT COMMENT '访问量（页面被访问的总次数）',
                                                   click_pv BIGINT COMMENT '页面总点击量',
                                                   data_source STRING COMMENT '数据来源，固定为"business_analyzer"（生意参谋）'
)
    COMMENT '页面访问及点击基础原始数据'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/ods/db_ods/ods_page_visit_base'
    tblproperties ("orc.compress"="snappy");

--2. 页面板块点击分布表（ods_page_block_click）
CREATE TABLE IF NOT EXISTS ods_page_block_click (
                                                    page_id STRING COMMENT '页面唯一标识',
                                                    block_id STRING COMMENT '板块唯一标识（如首页轮播区、商品推荐区）',
                                                    block_name STRING COMMENT '板块名称',
                                                    click_pv BIGINT COMMENT '板块点击量',
                                                    click_uv BIGINT COMMENT '板块点击人数',
                                                    guide_pay_amount DECIMAL(16,2) COMMENT '通过该板块引导的支付金额',
    collect_time STRING COMMENT '数据采集时间，格式：yyyy-MM-dd HH:mm:ss'
    )
    COMMENT '页面各板块点击分布原始数据'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall_10/ods/db_ods/ods_page_block_click'
    tblproperties ("orc.compress"="snappy");


--3. 页面数据趋势表（ods_page_data_trend）

CREATE TABLE IF NOT EXISTS ods_page_data_trend (
                                                   page_id STRING COMMENT '页面唯一标识',
                                                   stat_date STRING COMMENT '统计日期，格式：yyyy-MM-dd',
                                                   visitor_count BIGINT COMMENT '当日访客数',
                                                   click_uv BIGINT COMMENT '当日点击人数',
                                                   data_version STRING COMMENT '数据版本，如"v1.0"（用于区分数据更新批次）'
)
    COMMENT '页面近30天数据趋势原始数据'
    PARTITIONED BY (dt STRING )
    STORED AS ORC
    location '/warehouse/gd_gmall_10/ods/db_ods/ods_page_data_trend'
    tblproperties ("orc.compress"="snappy");


--4. 页面引导商品数据表（ods_page_guide_product）
CREATE TABLE IF NOT EXISTS ods_page_guide_product (
                                                      page_id STRING COMMENT '页面唯一标识',
                                                      product_id STRING COMMENT '被引导至的商品ID',
                                                      guide_count BIGINT COMMENT '引导次数（页面跳转至该商品的次数）',
                                                      guide_buyer_count BIGINT COMMENT '通过该引导下单的买家数',
                                                      guide_time STRING COMMENT '引导行为发生时间，格式：yyyy-MM-dd HH:mm:ss'
)
    COMMENT '页面引导至商品的原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式：yyyy-MM-dd')
    STORED AS ORC
    location '/warehouse/gd_gmall_10/ods/db_ods/ods_page_guide_product'
    tblproperties ("orc.compress"="snappy");

--5. 页面模块分布明细表（ods_page_module_detail）
CREATE TABLE IF NOT EXISTS ods_page_module_detail (
                                                      page_id STRING COMMENT '页面唯一标识',
                                                      module_id STRING COMMENT '模块唯一标识',
                                                      module_name STRING COMMENT '模块名称（如"新品推荐模块"、"优惠活动模块"）',
                                                      module_type STRING COMMENT '模块类型（如"推荐型"、"活动型"）',
                                                      expose_pv BIGINT COMMENT '模块曝光量',
                                                      interact_count BIGINT COMMENT '模块互动量（如点击、收藏等）',
                                                      stat_time STRING COMMENT '统计时间，格式：yyyy-MM-dd HH:mm:ss'
)
    COMMENT '页面各模块分布明细原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式：yyyy-MM-dd')
    STORED AS ORC
    LOCATION '/user/hive/warehouse/ods/db_ods/ods_page_module_detail'
    tblproperties ("orc.compress"="snappy");

select  count(1) from ods_page_module_detail;

