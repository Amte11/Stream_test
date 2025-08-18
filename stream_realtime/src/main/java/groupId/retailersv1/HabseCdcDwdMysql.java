package groupId.retailersv1;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Title: HabseCdcDwdMysql
 * Author: hyx
 * Package: groupId.retailersv1
 * Date: 2025/8/18 22:02
 * Description: Habse mysql kafka
 */
public class HabseCdcDwdMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    }
}
