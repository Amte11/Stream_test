set hive.exec.mode.local.auto=True;
CREATE DATABASE IF NOT EXISTS ds_path;
USE ds_path;

-- 1. 原始页面访问日志表（核心表）
drop table if exists ods_page_visit_log;
CREATE TABLE ods_page_visit_log (
                                    log_id BIGINT COMMENT '日志ID',
                                    user_id BIGINT COMMENT '用户ID',
                                    session_id STRING COMMENT '会话ID',
                                    page_url STRING COMMENT '页面URL',
                                    page_type STRING COMMENT '页面类型(shop/product/other)',
                                    page_id BIGINT COMMENT '页面ID',
                                    referer_url STRING COMMENT '来源页面URL',
                                    referer_page_id BIGINT COMMENT '来源页面ID',
                                    visit_time TIMESTAMP COMMENT '访问时间',
                                    stay_duration INT COMMENT '停留时长(秒)',
                                    device_type STRING COMMENT '设备类型(mobile/pc/applet)',
                                    os_type STRING COMMENT '操作系统(iOS/Android/Windows等)',
                                    browser_type STRING COMMENT '浏览器类型',
                                    ip_address STRING COMMENT 'IP地址',
                                    province STRING COMMENT '省份',
                                    city STRING COMMENT '城市',
                                    is_new_visitor TINYINT COMMENT '是否新访客(1是/0否)'
)
    COMMENT '原始页面访问日志'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    location '/warehouse/gd_gmall/ods/ods_page_visit_log/';

select count(1) from ods_page_visit_log;
-- 2. 原始用户信息表
drop table if exists ods_user_info;
CREATE TABLE ods_user_info (

                               user_id BIGINT COMMENT '用户ID',
                               user_name STRING COMMENT '用户名',
                               gender STRING COMMENT '性别',
                               age INT COMMENT '年龄',
                               register_time TIMESTAMP COMMENT '注册时间',
                               vip_level INT COMMENT 'VIP等级',
                               province STRING COMMENT '省份',
                               city STRING COMMENT '城市',
                               phone_prefix STRING COMMENT '手机号前3位'
)
    COMMENT '原始用户信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    location '/warehouse/gd_gmall/ods/ods_user_info/';

-- 3. 原始商品信息表
drop table if exists ods_product_info;
CREATE TABLE ods_product_info (
                                  product_id BIGINT COMMENT '商品ID',
                                  product_name STRING COMMENT '商品名称',
                                  category_id BIGINT COMMENT '类目ID',
                                  category_name STRING COMMENT '类目名称',
                                  brand_id BIGINT COMMENT '品牌ID',
                                  brand_name STRING COMMENT '品牌名称',
                                  price DECIMAL(18,2) COMMENT '商品价格',
                                  cost_price DECIMAL(18,2) COMMENT '成本价',
                                  status TINYINT COMMENT '状态(1上架/0下架)',
                                  create_time TIMESTAMP COMMENT '创建时间'
)
    COMMENT '原始商品信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall/ods/ods_product_info/';

-- 4. 原始店铺页面信息表
drop table if exists ods_shop_page_info;
CREATE TABLE ods_shop_page_info (
                                    page_id BIGINT COMMENT '页面ID',
                                    page_name STRING COMMENT '页面名称',
                                    page_url STRING COMMENT '页面URL',
                                    page_type STRING COMMENT '页面类型(home/activity/category/new等)',
                                    page_level TINYINT COMMENT '页面层级',
                                    parent_page_id BIGINT COMMENT '父页面ID',
                                    is_active TINYINT COMMENT '是否有效(1是/0否)',
                                    create_time TIMESTAMP COMMENT '创建时间'
)
    COMMENT '原始店铺页面信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall/ods/ods_shop_page_info/';

-- 5. 原始订单交易表（用于转化率计算）
drop table if exists ods_order_info;
CREATE TABLE ods_order_info (
                                order_id BIGINT COMMENT '订单ID',
                                user_id BIGINT COMMENT '用户ID',
                                product_id BIGINT COMMENT '商品ID',
                                order_amount DECIMAL(18,2) COMMENT '订单金额',
                                payment_amount DECIMAL(18,2) COMMENT '实付金额',
                                order_time TIMESTAMP COMMENT '下单时间',
                                payment_time TIMESTAMP COMMENT '支付时间',
                                order_status TINYINT COMMENT '订单状态'
)
    COMMENT '原始订单信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall/ods/ods_order_info/';

-- 6. 原始流量来源表
drop table if exists ods_traffic_source;
CREATE TABLE ods_traffic_source (
                                    source_id BIGINT COMMENT '来源ID',
                                    source_type STRING COMMENT '来源类型(search/social/ad等)',
                                    source_name STRING COMMENT '来源名称',
                                    source_url STRING COMMENT '来源URL',
                                    campaign_id BIGINT COMMENT '活动ID',
                                    campaign_name STRING COMMENT '活动名称'
)
    COMMENT '原始流量来源信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall/ods/ods_traffic_source/';

drop table if exists ods_marketing_activity;
CREATE TABLE ods_marketing_activity (
                                        activity_id BIGINT COMMENT '活动ID',
                                        activity_name STRING COMMENT '活动名称',
                                        start_time TIMESTAMP COMMENT '开始时间',
                                        end_time TIMESTAMP COMMENT '结束时间',
                                        activity_type STRING COMMENT '活动类型'
)comment '营销活动表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall/ods/ods_marketing_activity/';


drop table if exists ods_page_relationship;
CREATE TABLE ods_page_relationship (
                                       from_page_id BIGINT COMMENT '来源页面ID',
                                       to_page_id BIGINT COMMENT '去向页面ID',
                                       relation_type STRING COMMENT '关系类型',
                                       create_time TIMESTAMP COMMENT '创建时间'
)comment '页面跳转关系表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/gd_gmall/ods/ods_page_relationship/';