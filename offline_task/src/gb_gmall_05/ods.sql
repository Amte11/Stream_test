CREATE DATABASE IF NOT EXISTS gmall_05;
USE gmall_05;
SET hive.mapred.local.mem = 4096;
SET hive.auto.convert.join = false;


--一. 用户行为日志表（ods_user_behavior_log）
CREATE TABLE ods_user_behavior_log (
                                       log_id          STRING      COMMENT '日志唯一ID',
                                       user_id         STRING      COMMENT '用户ID',
                                       session_id      STRING      COMMENT '会话ID(用于识别同次访问)',
                                       goods_id        STRING      COMMENT '商品ID',
                                       behavior_type   STRING      COMMENT '行为类型: view-访问, favor-收藏, cart-加购',
                                       behavior_time   TIMESTAMP   COMMENT '行为发生时间(精确到秒)',
                                       device_id       STRING      COMMENT '设备ID',
                                       app_version     STRING      COMMENT 'APP版本号',
                                       page_url        STRING      COMMENT '页面URL',
                                       refer_page      STRING      COMMENT '来源页面URL',
                                       refer_goods_id  STRING      COMMENT '前序访问商品ID',
                                       stay_duration   INT         COMMENT '页面停留时长(秒)'
)
    COMMENT '用户行为日志表-核心事实表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
STORED AS ORC
location '/warehouse/gmall_05/ods/ods_user_behavior_log/'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);
--2. 订单交易事实表（ods_order_trade_fact）
CREATE TABLE ods_order_trade_fact (
                                      order_id        STRING      COMMENT '订单ID',
                                      trade_no        STRING      COMMENT '交易流水号',
                                      user_id         STRING      COMMENT '用户ID',
                                      shop_id         STRING      COMMENT '店铺ID',
                                      pay_time        TIMESTAMP   COMMENT '支付完成时间(精确到秒)',
                                      total_amount    DECIMAL(16,2) COMMENT '订单总金额',
                                      discount_amount DECIMAL(16,2) COMMENT '优惠金额',
                                      payment_amount  DECIMAL(16,2) COMMENT '实付金额',
                                      pay_channel     STRING      COMMENT '支付渠道',
                                      pay_status      TINYINT     COMMENT '支付状态(1成功 2失败)'
)
    COMMENT '订单交易事实表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
STORED AS ORC
location '/warehouse/gmall_05/ods/ods_order_trade_fact/'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

select count(1) from ods_order_trade_fact;
--3. 订单商品明细表（ods_order_goods_detail）
CREATE TABLE ods_order_goods_detail (
                                        detail_id       STRING      COMMENT '明细项ID',
                                        order_id        STRING      COMMENT '关联订单ID',
                                        goods_id        STRING      COMMENT '商品ID',
                                        goods_quantity  INT         COMMENT '购买数量',
                                        unit_price      DECIMAL(10,2) COMMENT '商品单价',
                                        total_price     DECIMAL(16,2) COMMENT '商品总金额',
                                        promotion_info  STRING      COMMENT '促销信息'
)
    COMMENT '订单商品明细表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
STORED AS ORC
location '/warehouse/gmall_05/ods/ods_order_goods_detail/'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);
--4. 商品核心属性表（ods_goods_core_attr）
CREATE TABLE ods_goods_core_attr (
                                     goods_id        STRING      COMMENT '商品ID',
                                     shop_id         STRING      COMMENT '店铺ID',
                                     goods_name      STRING      COMMENT '商品名称',
                                     category_l1     STRING      COMMENT '一级类目',
                                     category_l2     STRING      COMMENT '二级类目',
                                     category_l3     STRING      COMMENT '三级类目',
                                     is_traffic_item TINYINT     COMMENT '是否引流品(1是0否)',
                                     is_top_seller   TINYINT     COMMENT '是否热销品(1是0否)',
                                     base_price      DECIMAL(10,2) COMMENT '基础售价',
                                     goods_status    TINYINT     COMMENT '商品状态(1上架 0下架)',
                                     create_time     TIMESTAMP   COMMENT '创建时间',
                                     update_time     TIMESTAMP   COMMENT '最后更新时间'
)
    COMMENT '商品核心属性表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
STORED AS ORC
location '/warehouse/gmall_05/ods/ods_goods_core_attr/'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);
--5. 店铺基础信息表（ods_shop_base_info）
CREATE TABLE ods_shop_base_info (
                                    shop_id         STRING      COMMENT '店铺ID',
                                    shop_name       STRING      COMMENT '店铺名称',
                                    seller_user_id  STRING      COMMENT '商家用户ID',
                                    shop_level      TINYINT     COMMENT '店铺等级(1-5星)',
                                    shop_type       STRING      COMMENT '店铺类型(旗舰店/专卖店/专营店)',
                                    main_category   STRING      COMMENT '主营类目',
                                    create_date     DATE        COMMENT '开店日期'
)
    COMMENT '店铺基础信息表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
STORED AS ORC
location '/warehouse/gmall_05/ods/ods_shop_base_info/'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);
---6. 用户基础信息表（ods_user_base_info）
CREATE TABLE ods_user_base_info (
                                    user_id         STRING      COMMENT '用户ID',
                                    user_name       STRING      COMMENT '用户名',
                                    reg_channel     STRING      COMMENT '注册渠道(app/web)',
                                    reg_date        DATE        COMMENT '注册日期',
                                    last_login_time TIMESTAMP   COMMENT '最后登录时间',
                                    user_level      TINYINT     COMMENT '用户等级'
)
    COMMENT '用户基础信息表'
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyyMMdd')
STORED AS ORC
location '/warehouse/gmall_05/ods/ods_user_base'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);