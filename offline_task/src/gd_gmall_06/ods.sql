set hive.exec.mode.local.auto=True;
create database if not exists gmall_06;
use gmall_06;

--1. 商品基础信息表 (ods_sku_base_info)
CREATE TABLE IF NOT EXISTS ods_sku_base_info (
                                                 sku_id STRING COMMENT '商品货号（唯一标识）',
                                                 sku_title STRING COMMENT '商品标题',
                                                 sku_main_image STRING COMMENT '商品主图URL',
                                                 sku_sub_images STRING COMMENT '商品辅图URL（多图逗号分隔）',
                                                 category_id STRING COMMENT '类目ID',
                                                 category_name STRING COMMENT '类目名称',
                                                 category_path STRING COMMENT '类目层级路径（扩展）',
                                                 brand_id STRING COMMENT '品牌ID',
                                                 brand_name STRING COMMENT '品牌名称',
                                                 product_model STRING COMMENT '商品型号',
                                                 market_price DECIMAL(10,2) COMMENT '市场价',
    sale_price DECIMAL(10,2) COMMENT '销售价',
    cost_price DECIMAL(10,2) COMMENT '成本价（扩展）',
    weight DECIMAL(10,2) COMMENT '商品重量（kg）',
    color STRING COMMENT '颜色属性',
    size STRING COMMENT '尺寸属性',
    store_id STRING COMMENT '店铺ID',
    store_name STRING COMMENT '店铺名称（扩展）',
    supplier_id STRING COMMENT '供应商ID（扩展）',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    putaway_time TIMESTAMP COMMENT '上架时间（新增）',
    is_online TINYINT COMMENT '是否上架（1-是，0-否）',
    is_delete TINYINT COMMENT '是否删除（0-未删，1-已删）'
    )
    COMMENT 'ODS层-商品基础信息表'
    PARTITIONED BY (dt string)
    STORED AS PARQUET
    location '/warehouse/gmall_06/ods/ods_sku_base_info'
    TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
                  );
--2. 商品上新标签表 (ods_sku_new_arrival_tag)
CREATE TABLE IF NOT EXISTS ods_sku_new_arrival_tag (
                                                       id STRING COMMENT '记录ID',
                                                       sku_id STRING COMMENT '商品货号',
                                                       putaway_time TIMESTAMP COMMENT '上架时间（精确到秒）',
                                                       new_type STRING COMMENT '新品类型（普通新品/小黑盒新品）',
                                                       is_tmall_new TINYINT COMMENT '是否天猫新品（1-是，0-否）',
                                                       new_tag_audit_status STRING COMMENT '审核状态（待审/通过/驳回）',
                                                       new_tag_valid_days INT COMMENT '标签有效期（天）',
                                                       new_tag_start_time TIMESTAMP COMMENT '标签生效时间',
                                                       new_tag_end_time TIMESTAMP COMMENT '标签失效时间',
                                                       operator_id STRING COMMENT '操作人ID',
                                                       audit_time TIMESTAMP COMMENT '审核时间（扩展）',
                                                       reject_reason STRING COMMENT '驳回原因（扩展）',
                                                       platform STRING COMMENT '平台来源（天猫/淘宝等）',
                                                       create_time TIMESTAMP COMMENT '记录创建时间',
                                                       update_time TIMESTAMP COMMENT '记录更新时间'
)
    COMMENT 'ODS层-商品上新标签表'
    PARTITIONED BY (dt  string)
    STORED AS PARQUET
    location '/warehouse/gmall_06/ods/ods_sku_new_arrival_tag'
    TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
                  );
---3. 商品支付明细宽表 (ods_sku_payment_detail)
CREATE TABLE IF NOT EXISTS ods_sku_payment_detail (
                                                      payment_id STRING COMMENT '支付单ID',
                                                      order_id STRING COMMENT '订单ID',
                                                      sku_id STRING COMMENT '商品货号',
                                                      user_id STRING COMMENT '用户ID',
                                                      payment_amount DECIMAL(16,2) COMMENT '支付金额',
    payment_quantity INT COMMENT '支付数量',
    payment_time TIMESTAMP COMMENT '支付时间（精确到秒）',
    payment_type STRING COMMENT '支付方式',
    payment_status TINYINT COMMENT '支付状态（1-成功）',
    refund_status TINYINT COMMENT '退款状态（0-未退）',
    refund_amount DECIMAL(16,2) COMMENT '退款金额',
    refund_time TIMESTAMP COMMENT '退款时间（扩展）',
    store_id STRING COMMENT '店铺ID',
    promotion_id STRING COMMENT '促销活动ID（扩展）',
    channel STRING COMMENT '支付渠道（APP/PC）',
    create_time TIMESTAMP COMMENT '记录创建时间'
    )
    COMMENT 'ODS层-商品支付明细宽表'
    PARTITIONED BY (dt  string)
    STORED AS PARQUET
    location '/warehouse/gmall_06/ods/ods_sku_payment_detail'
    TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
                  );
--4. 商品全年上新记录表 (ods_sku_yearly_new_record)
CREATE TABLE IF NOT EXISTS ods_sku_yearly_new_record (
                                                         id STRING COMMENT '记录ID',
                                                         sku_id STRING COMMENT '商品货号',
                                                         putaway_date DATE COMMENT '上架日期（yyyy-MM-dd）',
                                                         putaway_year INT COMMENT '上架年份',
                                                         putaway_month INT COMMENT '上架月份',
                                                         putaway_quarter INT COMMENT '上架季度（扩展）',
                                                         putaway_week INT COMMENT '上架周数（扩展）',
                                                         new_type STRING COMMENT '新品类型',
                                                         initial_sale_price DECIMAL(10,2) COMMENT '上架时售价',
    initial_stock INT COMMENT '上架时库存（扩展）',
    first_payment_date DATE COMMENT '首次支付日期（扩展）',
    category_id STRING COMMENT '类目ID（扩展）',
    brand_id STRING COMMENT '品牌ID（扩展）',
    create_time TIMESTAMP COMMENT '记录创建时间'
    )
    COMMENT 'ODS层-商品全年上新记录表'
    PARTITIONED BY (dt  string)
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ods.db/ods_sku_yearly_new_record'
    TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
                  );
--5. 店铺基础信息表 (ods_store_base_info) - 新增
CREATE TABLE IF NOT EXISTS ods_store_base_info (
                                                   store_id STRING COMMENT '店铺ID',
                                                   store_name STRING COMMENT '店铺名称',
                                                   store_type STRING COMMENT '店铺类型（旗舰店/专卖店）',
                                                   seller_id STRING COMMENT '卖家ID',
                                                   platform STRING COMMENT '所属平台',
                                                   category_main STRING COMMENT '主营类目',
                                                   open_date DATE COMMENT '开店日期',
                                                   level TINYINT COMMENT '店铺等级',
                                                   contact_person STRING COMMENT '联系人（扩展）',
                                                   contact_phone STRING COMMENT '联系电话（扩展）',
                                                   create_time TIMESTAMP COMMENT '创建时间',
                                                   update_time TIMESTAMP COMMENT '更新时间'
)
    COMMENT 'ODS层-店铺基础信息表'
    PARTITIONED BY (dt  string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_06/ods/ods_store_base_info'
    TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
                  );

select  count(1) from ods_sku_new_arrival_tag;