package groupId.gdds04.ods;

import com.alibaba.fastjson.JSONObject;
import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import groupId.retailersv1.stream.utils.CdcSourceUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
/**
 * Title: MysqlCdcToFlinkGd04KafkaTopics
 * Author: hyx
 * Package: groupId.gdds04.ods
 * Date: 2025/8/28 22:47
 * Description: cdc to kafka
 */
public class MysqlCdcToFlinkGd04KafkaTopics {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcToFlinkGd04KafkaTopics.class);

    // Kafka 配置
    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DATABASE_NAME = "gd_gmall_04"; // 修改为实际的数据库名

    // 为每张表定义对应的Kafka主题名称（基于提供的配置）
    private static final String CHANNEL_CONFIG_TOPIC = "FlinkGd04_ods_channel_config_info";
    private static final String COLLECT_INFO_DETAIL_TOPIC = "FlinkGd04_ods_collect_info_detail";
    private static final String CONSULT_INFO_DETAIL_TOPIC = "FlinkGd04_ods_consult_info_detail";
    private static final String CUSTOMER_INFO_TOPIC = "FlinkGd04_ods_customer_info";
    private static final String GOODS_INFO_TOPIC = "FlinkGd04_ods_goods_info";
    private static final String MEMBERSHIP_INFO_DETAIL_TOPIC = "FlinkGd04_ods_membership_info_detail";
    private static final String PAYMENT_INFO_DETAIL_TOPIC = "FlinkGd04_ods_payment_info_detail";
    private static final String POPULARITY_BROADCASTINFO_DETAIL_TOPIC = "FlinkGd04_ods_popularity_broadcastinfo_detail";
    private static final String SEARCH_INFO_DETAIL_TOPIC = "FlinkGd04_ods_search_info_detail";
    private static final String SHOP_INFO_TOPIC = "FlinkGd04_ods_shop_info";
    private static final String VISITOR_INFO_DETAIL_TOPIC = "FlinkGd04_ods_visitor_info_detail";
    private static final String Z_LOG_TOPIC = "FlinkGd04_ods_z_log";

    // 定义侧输出流标签用于新老用户分流
    private static final OutputTag<String> NEW_USER_TAG = new OutputTag<String>("new-user") {};
    private static final OutputTag<String> OLD_USER_TAG = new OutputTag<String>("old-user") {};

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        // 创建MySQL CDC源，采集gd_gmall_04数据库的所有表
        MySqlSource<String> mySqlSource = CdcSourceUtils.getMySQLCdcSource(
                DATABASE_NAME,
                DATABASE_NAME + ".*", // 采集所有表
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                "5000-6000",
                StartupOptions.initial()
        );

        // 读取CDC数据流
        DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );

        // 处理数据流：打印原始数据、进行新老用户校验、按表名分发到不同Kafka主题
        SingleOutputStreamOperator<String> processedStream = cdcStream
                .map(jsonStr -> {
                    // 打印原始数据查看结构
                    LOG.info("Original CDC Data: {}", jsonStr);
                    return JSONObject.parseObject(jsonStr);
                })
                .process(new ProcessFunction<JSONObject, String>() {
                    // 存储已知访客ID的集合（实际生产环境中应使用外部存储如Redis）
                    private Set<String> visitorIds = new HashSet<>();

                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        // 获取表名
                        String tableName = jsonObject.getJSONObject("source").getString("table");

                        // 提取数据部分
                        JSONObject data;
                        String op = jsonObject.getString("op");

                        switch (op) {
                            case "r": // 读取快照数据
                            case "c": // 插入操作
                                data = jsonObject.getJSONObject("after");
                                break;
                            case "u": // 更新操作
                                data = jsonObject.getJSONObject("after");
                                break;
                            case "d": // 删除操作
                                data = jsonObject.getJSONObject("before");
                                break;
                            default:
                                return;
                        }

                        // 对于包含visitor_id的表，进行新老用户校验
                        if (data.containsKey("visitor_id")) {
                            String visitorId = data.getString("visitor_id");
                            boolean isNewVisitor = !visitorIds.contains(visitorId); // 判断是否为新访客

                            // 更新访客集合
                            visitorIds.add(visitorId);

                            // 更新is_new_visitor字段（如果存在该字段）
                            if (data.containsKey("is_new_visitor")) {
                                data.put("is_new_visitor", isNewVisitor ? 1 : 0);

                                // 根据新老用户分流到侧输出流
                                if (isNewVisitor) {
                                    context.output(NEW_USER_TAG, data.toJSONString());
                                } else {
                                    context.output(OLD_USER_TAG, data.toJSONString());
                                }
                            }
                        }

                        // 添加表名标识到数据中
                        data.put("_table", tableName);

                        // 发送到主输出流
                        collector.collect(data.toJSONString());

                        // 根据表名分发到不同的Kafka主题（在sink部分处理）
                    }
                });

        // 按表名分发数据到不同的Kafka主题
        // ods_channel_config_info 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "channel_config_info".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, CHANNEL_CONFIG_TOPIC))
                .name("sink-channel-config-to-kafka");

        // ods_collect_info_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "collect_info_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, COLLECT_INFO_DETAIL_TOPIC))
                .name("sink-collect-info-detail-to-kafka");

        // ods_consult_info_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "consult_info_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, CONSULT_INFO_DETAIL_TOPIC))
                .name("sink-consult-info-detail-to-kafka");

        // ods_customer_info 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "customer_info".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, CUSTOMER_INFO_TOPIC))
                .name("sink-customer-info-to-kafka");

        // ods_goods_info 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "goods_info".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, GOODS_INFO_TOPIC))
                .name("sink-goods-info-to-kafka");

        // ods_membership_info_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "membership_info_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, MEMBERSHIP_INFO_DETAIL_TOPIC))
                .name("sink-membership-info-detail-to-kafka");

        // ods_payment_info_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "payment_info_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, PAYMENT_INFO_DETAIL_TOPIC))
                .name("sink-payment-info-detail-to-kafka");

        // ods_popularity_broadcastinfo_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "popularity_broadcastinfo_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, POPULARITY_BROADCASTINFO_DETAIL_TOPIC))
                .name("sink-popularity-broadcastinfo-detail-to-kafka");

        // ods_search_info_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "search_info_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, SEARCH_INFO_DETAIL_TOPIC))
                .name("sink-search-info-detail-to-kafka");

        // ods_shop_info 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "shop_info".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, SHOP_INFO_TOPIC))
                .name("sink-shop-info-to-kafka");

        // ods_visitor_info_detail 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "visitor_info_detail".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, VISITOR_INFO_DETAIL_TOPIC))
                .name("sink-visitor-info-detail-to-kafka");

        // ods_z_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "z_log".equals(tableName);
                    } catch (Exception e) {
                        LOG.error("Error parsing JSON: {}", jsonStr, e);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, Z_LOG_TOPIC))
                .name("sink-z-log-to-kafka");

        env.execute("MySQL CDC to FlinkGd03 Kafka Topics");
    }
}
