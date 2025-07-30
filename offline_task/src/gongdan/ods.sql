set hive.exec.mode.local.auto=True;
create database if not exists ds_path;
use ds_path;

-- 1. 用户行为日志表 (核心表)
CREATE TABLE osd_user_log (
                              log_id BIGINT COMMENT '日志ID',
                              session_id STRING COMMENT '会话ID',
                              user_id BIGINT COMMENT '用户ID',
                              page_id STRING COMMENT '页面ID',
                              page_type STRING COMMENT '页面类型: shop_page/store_item/store_other',
                              refer_page_id STRING COMMENT '来源页面ID',
                              device_type STRING COMMENT '设备类型: wireless/pc',
                              visit_time TIMESTAMP COMMENT '访问时间',
                              stay_duration INT COMMENT '停留时长(秒)',
                              is_order INT COMMENT '是否下单: 0否/1是'
) PARTITIONED BY (dt STRING)
STORED AS ORC TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 2. 商品页面表 (包含额外页字段)
CREATE TABLE ods_product_page (
                                  page_id STRING COMMENT '页面ID',
                                  product_id STRING COMMENT '商品ID',
                                  product_name STRING COMMENT '商品名称',
                                  category STRING COMMENT '商品类目',
                                  page_section STRING COMMENT '页面区域: header/content/sidebar/footer', -- 新增页字段
                                  create_time TIMESTAMP COMMENT '创建时间'
) STORED AS ORC;

-- 3. 店铺页面表
CREATE TABLE ods_shop_page (
                               page_id STRING COMMENT '页面ID',
                               page_name STRING COMMENT '页面名称',
                               page_type STRING COMMENT '页面类型: home/activity/category/new',
                               template_id STRING COMMENT '模板ID'
) STORED AS ORC;

-- 4. 店内路径表 (聚合表)
CREATE TABLE ods_store_path (
                                from_page_id STRING COMMENT '来源页面ID',
                                to_page_id STRING COMMENT '去向页面ID',
                                path_count BIGINT COMMENT '路径数量',
                                avg_stay_duration DOUBLE COMMENT '平均停留时长',
                                conversion_rate DOUBLE COMMENT '转化率'
) PARTITIONED BY (dt STRING, device_type STRING)
STORED AS ORC;

-- 5. PC端流量入口表
CREATE TABLE ods_traffic_source (
                                    source_page_id STRING COMMENT '来源页面ID',
                                    source_type STRING COMMENT '来源类型: direct/search/social',
                                    session_count BIGINT COMMENT '会话数',
                                    avg_session_duration DOUBLE COMMENT '平均会话时长'
) PARTITIONED BY (dt STRING)
STORED AS ORC;