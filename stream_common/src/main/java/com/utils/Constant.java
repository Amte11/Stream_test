package com.utils;

/**
 * Title: Constant
 * Author: hyx
 * Package: com.utils
 * Date: 2025/8/22 22:13
 * Description:
 */
public class Constant {
    private Constant() {}

    // Kafka相关
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "ods_page_log";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_cart_info";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL="dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS="dwd_trade_order_pay_suc_detail";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "cdh01:9092,cdh02:9092"; // Kafka集群地址

    // Doris相关
    public static final String DORIS_DATABASE = "gmall2025_realtime"; // Doris数据库名
    public static final String DORIS_FE_NODES = "cdh01:8031"; // Doris FE节点
}
