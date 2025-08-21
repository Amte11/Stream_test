package groupId.retailersv1.dws;

import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.SqlUtil;
import groupId.retailersv1.func.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Title: DbusDwsTrafficSourceKeywordPageViewToDoris
 * Author: hyx
 * Package: groupId.retailersv1.dws
 * Date: 2025/8/21 21:56
 * Description: 流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class DbusDwsTrafficSourceKeywordPageViewToDoris {
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));


        tableEnv.createTemporarySystemFunction("KeywordUDTF", KeywordUDTF.class);

        // 1. 读取 页面日志
        tableEnv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " ts bigint, " +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et\n" +
                ")" + SqlUtil.getKafka(kafka_page_topic, "retailersv_page_log"));

//        tableEnv.executeSql("select * from page_log ").print();

        // 2. 读取搜索关键词
        Table kwTable = tableEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page_log " +
                "where  page['last_page_id'] ='search' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("kw_table", kwTable);

//        tableEnv.executeSql("select * from kw_table ").print();
        Table keywordTable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                ", lateral table(keywordUDTF(kw)) t(keyword) ");
        tableEnv.createTemporaryView("keyword_table", keywordTable);

//        keywordTable.execute().print();
        // . 开窗聚和 tvf
        Table result = tableEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyyMMdd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '3' seconds ) ) " +
                "group by window_start, window_end, keyword ");

        result.execute().print();



    }
}
