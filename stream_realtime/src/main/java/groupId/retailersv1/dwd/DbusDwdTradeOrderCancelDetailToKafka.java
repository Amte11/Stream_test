package groupId.retailersv1.dwd;

import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Title: DbusDwdTradeOrderCancelDetailToKafka
 * Author: hyx
 * Package: groupId.retailersv1.dwd
 * Date: 2025/8/20 21:42
 * Description: DWD 层 - 取消订单明细事实表
 * 从 ODS CDC 数据中提取 order_status 从 '1001' → '1003' 的订单，
 * 关联 dwd_trade_order_detail 表，生成取消订单明细宽表，写入 upsert-kafka。
 */
public class DbusDwdTradeOrderCancelDetailToKafka {

    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_CANCEL_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.cancel.detail");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态 TTL：30分钟 + 5秒
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        env.setStateBackend(new MemoryStateBackend());

        // 2. 创建 ODS CDC 表（Debezium 格式）
        tableEnv.executeSql("CREATE TABLE ods_all_cdc (" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv1_dwd_order_cancel_detail"));

        // 3. 筛选取消订单数据（order_info 表：1001 → 1003）
        Table orderCancel = tableEnv.sqlQuery("SELECT " +
                " `after`['id'] AS id, " +
                " `after`['operate_time'] AS operate_time, " +
                " `ts_ms` " +
                "FROM ods_all_cdc " +
                "WHERE `source`['table'] = 'order_info' " +
                "  AND `op` = 'u' " +
                "  AND `before`['order_status'] = '1001' " +
                "  AND `after`['order_status'] = '1003'");

        tableEnv.createTemporaryView("order_cancel", orderCancel);
       // orderCancel.execute().print(); // 看是否输出


        // 4. 创建 DWD 下单明细表（来自 Kafka）
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL, "retailersv1_dwd_order_cancel_detail"));
        // 可选：调试
        //tableEnv.sqlQuery("SELECT * FROM dwd_trade_order_detail ").execute().print();

        // 5. 关联：订单明细 + 取消事件
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(FROM_UNIXTIME(cast(oc.operate_time as bigint) / 1000), 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts_ms " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        // 可选：调试输出结果
         result.execute().print();

        // 6. 创建 DWD 取消订单明细结果表（Upsert Kafka）
        tableEnv.executeSql(
                "CREATE TABLE " + DWD_TRADE_ORDER_CANCEL_DETAIL + " (" +
                        "id STRING PRIMARY KEY NOT ENFORCED, " +
                        "order_id STRING, " +
                        "user_id STRING, " +
                        "sku_id STRING, " +
                        "sku_name STRING, " +
                        "province_id STRING, " +
                        "activity_id STRING, " +
                        "activity_rule_id STRING, " +
                        "coupon_id STRING, " +
                        "date_id STRING, " +
                        "cancel_time STRING, " +
                        "sku_num STRING, " +
                        "split_original_amount STRING, " +
                        "split_activity_amount STRING, " +
                        "split_coupon_amount STRING, " +
                        "split_total_amount STRING, " +
                        "ts BIGINT" +
                        ") " + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_CANCEL_DETAIL));

        // 7. 写入结果
        //result.executeInsert(DWD_TRADE_ORDER_CANCEL_DETAIL);
    }
}