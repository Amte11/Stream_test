package groupId.gdds04.dwd;

import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Title: DwdTradeOrderDetail2Kafka
 * Author: hyx
 * Package: groupId.gdds04.dwd
 * Date: 2025/8/27 22:28
 * Description:
 */
public class DwdTradeOrderDetail2Kafka {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_DETAIL_TASK3 = ConfigUtils.getString("kafka.task.dwd.trade.order.detail.3");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 1. ODS 层 Kafka 源
        tableEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_dwd_order_detail"));

        // 2. 订单明细
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['sku_name'] sku_name," +
                        "   cast(`after`['order_price'] as decimal(16,2)) order_price," + // 分摊原始总金额
                        "`after`['sku_num'] sku_num," +
                        "`after`['create_time'] create_time," +
                        "`after`['order_status'] order_status," +
                        "ts_ms " +
                        "from ods_ecommerce_order " +
                        " where `source`['table'] ='order_detail'");
        tableEnv.createTemporaryView("order_detail", orderDetail);


//        tableEnv.executeSql("select * from order_detail").print();
        // 3. 订单表（拿 user_id, province_id）
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['province_id'] province_id " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='order_info' ");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 4. 商品表（拿 category, trademark）
        Table skuInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['category3_id'] category_id," +
                        "`after`['tm_id'] trademark_id," +
                        "`after`['sku_name'] sku_name " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='sku_info' ");
        tableEnv.createTemporaryView("sku_info", skuInfo);

        // 5. join 三张表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "si.category_id," +
                        " '' as category_name," +
                        "si.trademark_id," +
                        "cast(od.order_price as decimal(10,2)) as order_price," +
                        "cast(od.sku_num as int) as sku_num," +
                        "od.create_time," +
                        "date_format(FROM_UNIXTIME(cast(od.create_time as bigint)/1000), 'yyyy-MM-dd') as dt " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join sku_info si on od.sku_id=si.id");

        result.execute().print();

        // 6. 目标 DWD 表
        tableEnv.executeSql(
                "create table " + DWD_TRADE_ORDER_DETAIL_TASK3 + "(" +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "category_id string," +
                        "category_name string," +
                        "trademark_id string," +
                        "order_price decimal(10,2)," +
                        "sku_num int," +
                        "order_status string," +
                        "create_time string," +
                        "dt string," +
                        "primary key(order_id) not enforced " +
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_DETAIL_TASK3));

        // 7. 写入 Kafka
//        result.executeInsert(DWD_TRADE_ORDER_DETAIL_TASK3);
    }
}
