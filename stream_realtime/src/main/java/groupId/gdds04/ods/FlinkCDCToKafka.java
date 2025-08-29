package groupId.gdds04.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.json.JSONObject;

import java.util.Properties;

/**
 * Title: FlinkCDCToKafka
 * Author: hyx
 * Package: groupId.gdds04.ods
 * Date: 2025/8/29 15:12
 * Description: cdc to kafka
 */
public class FlinkCDCToKafka {
    private static final OutputTag<String> VISITOR_INFO_DETAIL_TAG = new OutputTag<String>("visitor_info_detail") {};
    private static final OutputTag<String> SHOP_INFO_TAG = new OutputTag<String>("shop_info") {};
    private static final OutputTag<String> SEARCH_INFO_DETAIL_TAG = new OutputTag<String>("search_info_detail") {};
    private static final OutputTag<String> POPULARITY_BROADCASTINFO_DETAIL_TAG = new OutputTag<String>("popularity_broadcastinfo_detail") {};
    private static final OutputTag<String> PAYMENT_INFO_DETAIL_TAG = new OutputTag<String>("payment_info_detail") {};
    private static final OutputTag<String> MEMBERSHIP_INFO_DETAIL_TAG = new OutputTag<String>("membership_info_detail") {};
    private static final OutputTag<String> GOODS_INFO_TAG = new OutputTag<String>("goods_info") {};
    private static final OutputTag<String> CUSTOMER_INFO_TAG = new OutputTag<String>("customer_info") {};
    private static final OutputTag<String> CONSULT_INFO_DETAIL_TAG = new OutputTag<String>("consult_info_detail") {};
    private static final OutputTag<String> COLLECT_INFO_DETAIL_TAG = new OutputTag<String>("collect_info_detail") {};
    private static final OutputTag<String> CHANNEL_CONFIG_INFO_TAG = new OutputTag<String>("channel_config_info") {};
    private static final OutputTag<String> CART_INFO_DETAIL_TAG = new OutputTag<String>("cart_info_detail") {};

    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 配置 MySQL CDC Source - 监控所有相关表
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("cdh01")
                .port(3306)
                .databaseList("gd_gmall_04")
                .tableList(
                        "gd_gmall_04.visitor_info_detail",
                        "gd_gmall_04.shop_info",
                        "gd_gmall_04.search_info_detail",
                        "gd_gmall_04.popularity_broadcastinfo_detail",
                        "gd_gmall_04.payment_info_detail",
                        "gd_gmall_04.membership_info_detail",
                        "gd_gmall_04.goods_info",
                        "gd_gmall_04.customer_info",
                        "gd_gmall_04.consult_info_detail",
                        "gd_gmall_04.collect_info_detail",
                        "gd_gmall_04.channel_config_info",
                        "gd_gmall_04.cart_info_detail"
                )
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 输出 JSON 格式
                .startupOptions(StartupOptions.initial()) // 第一次跑先全量，再实时增量
                .build();

        // 3. 获取 CDC 数据流
        DataStream<String> mysqlStream = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 4. 处理数据流，根据表名分流
        SingleOutputStreamOperator<String> processedStream = mysqlStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject source = json.getJSONObject("source");
                    String table = source.getString("table");

                    // 添加处理时间戳
                    json.put("process_time", System.currentTimeMillis());

                    // 根据表名分发到不同的侧输出流
                    switch (table) {
                        case "visitor_info_detail":
                            ctx.output(VISITOR_INFO_DETAIL_TAG, json.toString());
                            break;
                        case "shop_info":
                            ctx.output(SHOP_INFO_TAG, json.toString());
                            break;
                        case "search_info_detail":
                            ctx.output(SEARCH_INFO_DETAIL_TAG, json.toString());
                            break;
                        case "popularity_broadcastinfo_detail":
                            ctx.output(POPULARITY_BROADCASTINFO_DETAIL_TAG, json.toString());
                            break;
                        case "payment_info_detail":
                            ctx.output(PAYMENT_INFO_DETAIL_TAG, json.toString());
                            break;
                        case "membership_info_detail":
                            ctx.output(MEMBERSHIP_INFO_DETAIL_TAG, json.toString());
                            break;
                        case "goods_info":
                            ctx.output(GOODS_INFO_TAG, json.toString());
                            break;
                        case "customer_info":
                            ctx.output(CUSTOMER_INFO_TAG, json.toString());
                            break;
                        case "consult_info_detail":
                            ctx.output(CONSULT_INFO_DETAIL_TAG, json.toString());
                            break;
                        case "collect_info_detail":
                            ctx.output(COLLECT_INFO_DETAIL_TAG, json.toString());
                            break;
                        case "channel_config_info":
                            ctx.output(CHANNEL_CONFIG_INFO_TAG, json.toString());
                            break;
                        case "cart_info_detail":
                            ctx.output(CART_INFO_DETAIL_TAG, json.toString());
                            break;
                        default:
                            out.collect(json.toString()); // 未知表输出到主流
                    }
                } catch (Exception e) {
                    // 异常处理，输出到主流
                    out.collect(value);
                }
            }
        });

        // 5. 配置 Kafka Producer 属性
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092,cdh02:9092,cdh03:9092");

        // 6. 为每个表创建 Kafka Sink
        FlinkKafkaProducer<String> visitorInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_visitor_info_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> shopInfoSink = new FlinkKafkaProducer<>(
                "ods_shop_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> searchInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_search_info_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> popularityBroadcastInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_popularity_broadcastinfo_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> paymentInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_payment_info_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> membershipInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_membership_info_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> goodsInfoSink = new FlinkKafkaProducer<>(
                "ods_goods_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> customerInfoSink = new FlinkKafkaProducer<>(
                "ods_customer_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> consultInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_consult_info_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> collectInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_collect_info_detail",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> channelConfigInfoSink = new FlinkKafkaProducer<>(
                "ods_channel_config_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> cartInfoDetailSink = new FlinkKafkaProducer<>(
                "ods_cart_info_detail",
                new SimpleStringSchema(),
                props
        );

        // 7. 将侧输出流连接到对应的Kafka主题
        processedStream.getSideOutput(VISITOR_INFO_DETAIL_TAG).addSink(visitorInfoDetailSink).name("Visitor Info Detail Sink");
        processedStream.getSideOutput(SHOP_INFO_TAG).addSink(shopInfoSink).name("Shop Info Sink");
        processedStream.getSideOutput(SEARCH_INFO_DETAIL_TAG).addSink(searchInfoDetailSink).name("Search Info Detail Sink");
        processedStream.getSideOutput(POPULARITY_BROADCASTINFO_DETAIL_TAG).addSink(popularityBroadcastInfoDetailSink).name("Popularity Broadcast Info Detail Sink");
        processedStream.getSideOutput(PAYMENT_INFO_DETAIL_TAG).addSink(paymentInfoDetailSink).name("Payment Info Detail Sink");
        processedStream.getSideOutput(MEMBERSHIP_INFO_DETAIL_TAG).addSink(membershipInfoDetailSink).name("Membership Info Detail Sink");
        processedStream.getSideOutput(GOODS_INFO_TAG).addSink(goodsInfoSink).name("Goods Info Sink");
        processedStream.getSideOutput(CUSTOMER_INFO_TAG).addSink(customerInfoSink).name("Customer Info Sink");
        processedStream.getSideOutput(CONSULT_INFO_DETAIL_TAG).addSink(consultInfoDetailSink).name("Consult Info Detail Sink");
        processedStream.getSideOutput(COLLECT_INFO_DETAIL_TAG).addSink(collectInfoDetailSink).name("Collect Info Detail Sink");
        processedStream.getSideOutput(CHANNEL_CONFIG_INFO_TAG).addSink(channelConfigInfoSink).name("Channel Config Info Sink");
        processedStream.getSideOutput(CART_INFO_DETAIL_TAG).addSink(cartInfoDetailSink).name("Cart Info Detail Sink");

        // 8. 启动作业
        env.execute("ODS MySQL CDC → Kafka");
    }
}
