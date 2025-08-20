package groupId.retailersv1.dwd;

import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Title: DbusDwdTradeCartAdd2Kafka
 * Author: hyx
 * Package: groupId.retailersv1.dwd
 * Date: 2025/8/20 19:45
 * Description: 交易域加购事务事实表
 */
public class DbusDwdTradeCartAddToKafka {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_CART_INFO = ConfigUtils.getString("kafka.dwd.cart.info");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE ods_all_cdc (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime()\n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv1_ods_all_cdc"));
        //tableEnv.executeSql("select * from ods_all_cdc where `source`['table'] = 'cart_info' ").print();

        Table cartInfo = tableEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                " if(`op` = 'r',cast(`after`['sku_num'] as int),(cast(`after`['sku_num'] as int) - cast(`before`['sku_num'] as int))) sku_num,\n" +
                "ts_ms\n" +
                "from ods_all_cdc\n" +
                "where `source`['table'] = 'cart_info'\n" +
                "and (\n" +
                "(`op` = 'r')\n" +
                "or\n" +
                "(`op` = 'u' and `before`['sku_num'] is not null and (cast(`after`['sku_num'] as int) > cast(`before`['sku_num'] as int)) )\n" +
                ")");
//        cartInfo.execute().print();

        tableEnv.executeSql("CREATE TABLE "+DWD_CART_INFO+" (\n" +
                "      id String,\n" +
                "      user_id string,\n" +
                "      sku_id string,\n" +
                "      sku_num INT,\n" +
                "      ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SqlUtil.getUpsertKafkaDDL(DWD_CART_INFO));
        cartInfo.executeInsert(DWD_CART_INFO);

    }
}
