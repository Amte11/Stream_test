package groupId.retailersv1.dwd;

import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Title: DbusDwdTradeOrderCancelDetailToKafka
 * Author: hyx
 * Package: groupId.retailersv1.dwd
 * Date: 2025/8/20 21:42
 * Description: 取消订单事务事实表
 */
public class DbusDwdTradeOrderCancelDetailToKafka {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_CANCEL_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.cancel.detail");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());

        tableEnv.executeSql("CREATE TABLE ods_all_cdc (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_dwd_order_cancel_detail"));

        // 2. 从 topic_db 过滤出订单取消数据
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `ts_ms` " +
                "from ods_all_cdc " +
                "where `source`['table']='order_info' " +
                "and `op` = 'u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        orderCancel.execute().print();

        // 3. 读取 dwd 层下单事务事实表数据
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
                        ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL, "retailersv_dwd_order_cancel_detail"));

        //tableEnv.executeSql("select * from dwd_trade_order_detail").print();
        // 4. 订单取消表和下单表进行 join
        Table result = tableEnv.sqlQuery("select\n" +
                "od.detail_id as id,\n" +
                "od.order_id,\n" +
                "oc.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oc.province_id,\n" +
                "oda.activity_id,\n" +
                "oda.activity_rule_id,\n" +
                "odc.coupon_id,\n" +
                "date_format(from_unixtime(cast(oc.cancel_time as bigint) / 1000), 'yyyy-MM-dd') as date_id,\n" +
                "oc.cancel_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "cast(0 as string) as split_activity_amount,\n" +  // 取消订单暂不计算活动分摊
                "cast(0 as string) as split_coupon_amount,\n" +    // 取消订单暂不计算优惠券分摊
                "od.split_original_amount as split_total_amount,\n" +
                "oc.cancel_ts as ts\n" +
                "from order_detail od\n" +
                "join order_cancel oc on od.order_id = oc.order_id\n" +  // 内连接：只保留取消订单的明细
                "left join order_detail_activity oda \n" +
                "on od.order_id = oda.activity_order_id and od.detail_id = oda.activity_detail_id\n" +
                "left join order_detail_coupon odc \n" +
                "on od.order_id = odc.coupon_order_id and od.detail_id = odc.coupon_detail_id"
        );
        result.execute().print();
        // 5. 写出
        tableEnv.executeSql(
                "create table "+DWD_TRADE_ORDER_CANCEL_DETAIL+"(" +
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
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint ," +
                        "primary key(id) not enforced " +
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_CANCEL_DETAIL));

        //result.executeInsert(DWD_TRADE_ORDER_CANCEL_DETAIL);

    }
}
