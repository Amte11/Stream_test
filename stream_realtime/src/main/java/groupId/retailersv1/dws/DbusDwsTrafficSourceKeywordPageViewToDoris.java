package groupId.retailersv1.dws;

import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.SqlUtil;
import groupId.retailersv1.func.KeywordUDTF;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
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
    private static final String DORIS_FE_NODES = ConfigUtils.getString("doris.fe.nodes");
    private static final String DORIS_DATABASE = ConfigUtils.getString("doris.database");


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(3));
        env.setStateBackend(new MemoryStateBackend());


        tableEnv.createTemporarySystemFunction("keywordUDTF", KeywordUDTF.class);

        // 1. 读取 页面日志
        tableEnv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " ts bigint, " +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et - INTERVAL '5' SECOND\n" +
                ")" + SqlUtil.getKafka(kafka_page_topic, "retailersv_page_log"));

       // tableEnv.executeSql("select * from page_log ").print();

        // 2. 读取搜索关键词
        Table kwTable = tableEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page_log " +
                "where  page['last_page_id'] ='search' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("kw_table", kwTable);

       //tableEnv.executeSql("select * from kw_table ").print();

        Table keywordTable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                ", lateral table(keywordUDTF(kw)) t(keyword) ");
        tableEnv.createTemporaryView("keyword_table", keywordTable);

        //keywordTable.execute().print();
        // . 开窗聚和 tvf
        Table result = tableEnv.sqlQuery(
                "SELECT \n" +
                        // 窗口起始时间（处理时间）
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        // 窗口结束时间（处理时间）
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        // 统计日期（基于窗口起始时间的处理时间）
                        "date_format(window_start, 'yyyyMMdd') AS cur_date,\n" +
                        "keyword,\n" +
                        "count(*) AS keyword_count \n" +
                        // 基于处理时间pt创建滚动窗口
                        "FROM table(tumble(table keyword_table, descriptor(et), interval '5' SECOND  )) \n" +
                        "GROUP BY window_start, window_end, keyword"
        );
       //result.execute().print();

        // 5. 写出到 doris 中
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ") with (" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + DORIS_FE_NODES + "'," +
                " 'table.identifier' = '" + DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                " 'username' = 'root'," +
                " 'password' = '', " +
                " 'sink.properties.format' = 'json', " +
                " 'sink.properties.read_json_by_line' = 'true', " +
                " 'sink.buffer-count' = '4', " +
                " 'sink.buffer-size' = '4086'," +
                " 'sink.enable-2pc' = 'false' " +
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window");


    }
}
