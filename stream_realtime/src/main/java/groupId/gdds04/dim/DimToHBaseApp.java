package groupId.gdds04.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Title: DimToHBaseApp
 * Author: hyx
 * Package: groupId.gdds04.dim
 * Date: 2025/8/29 15:26
 * Description: 维度数据写入HBase作业：消费ODS层维度数据，写入HBase维度表
 */
public class DimToHBaseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        // Kafka 配置
        String kafkaBootstrap = "cdh01:9092,cdh02:9092,cdh03:9092";

        // 创建 Kafka Source
        KafkaSource<String> shopInfoSource = createKafkaSource(kafkaBootstrap, "ods_shop_info", "flink_dim_shop");
        KafkaSource<String> customerInfoSource = createKafkaSource(kafkaBootstrap, "ods_customer_info", "flink_dim_customer");
        KafkaSource<String> goodsInfoSource = createKafkaSource(kafkaBootstrap, "ods_goods_info", "flink_dim_goods");
        KafkaSource<String> membershipInfoSource = createKafkaSource(kafkaBootstrap, "ods_membership_info_detail", "flink_dim_membership");
        KafkaSource<String> visitorInfoSource = createKafkaSource(kafkaBootstrap, "ods_visitor_info_detail", "flink_dim_visitor");
        KafkaSource<String> channelConfigInfoSource = createKafkaSource(kafkaBootstrap, "ods_channel_config_info", "flink_dim_channel");

        // 消费各维度主题数据
        DataStream<String> shopInfoStream = env.fromSource(shopInfoSource, WatermarkStrategy.noWatermarks(), "Shop Info Source");
        DataStream<String> customerInfoStream = env.fromSource(customerInfoSource, WatermarkStrategy.noWatermarks(), "Customer Info Source");
        DataStream<String> goodsInfoStream = env.fromSource(goodsInfoSource, WatermarkStrategy.noWatermarks(), "Goods Info Source");
        DataStream<String> membershipInfoStream = env.fromSource(membershipInfoSource, WatermarkStrategy.noWatermarks(), "Membership Info Source");
        DataStream<String> visitorInfoStream = env.fromSource(visitorInfoSource, WatermarkStrategy.noWatermarks(), "Visitor Info Source");
        DataStream<String> channelConfigInfoStream = env.fromSource(channelConfigInfoSource, WatermarkStrategy.noWatermarks(), "Channel Config Info Source");

        // ==================== 1. Shop Info ====================
        shopInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table table;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                table = hbaseConn.getTable(TableName.valueOf("dim_shop"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");
                    String opType = json.getString("op");

                    if (after == null) {
                        System.err.println("After is null: " + value);
                        return null;
                    }

                    String shopId = safeGetString(after, "shop_id");
                    if (shopId == null || shopId.isEmpty()) {
                        System.err.println("Shop ID missing: " + value);
                        return null;
                    }

                    if ("d".equals(opType)) {
                        System.out.println("Delete operation for shop: " + shopId);
                        return "Shop deleted: " + shopId;
                    }

                    Put put = new Put(Bytes.toBytes(shopId));
                    addColumnIfNotNull(put, "info", "shop_name", after);
                    addColumnIfNotNull(put, "info", "merchant_id", after);
                    addColumnIfNotNull(put, "info", "shop_category", after);
                    addColumnIfNotNull(put, "info", "open_date", after);
                    addColumnIfNotNull(put, "info", "is_direct_bus", after);
                    addColumnIfNotNull(put, "info", "business_scope", after);

                    table.put(put);
                    return "Shop written to HBase: " + shopId;

                } catch (Exception e) {
                    System.err.println("Failed to process shop info: " + value);
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }

            private String safeGetString(JSONObject obj, String key) {
                if (obj == null) return null;
                Object val = obj.get(key);
                return val == null ? null : val.toString();
            }

            private void addColumnIfNotNull(Put put, String family, String qualifier, JSONObject obj) {
                String value = safeGetString(obj, qualifier);
                if (value != null && !value.isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
        }).print().name("Shop Info HBase Sink");

        // ==================== 2. Customer Info ====================
        customerInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table table;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                table = hbaseConn.getTable(TableName.valueOf("dim_customer"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");
                    String opType = json.getString("op");

                    if (after == null) {
                        System.err.println("After is null: " + value);
                        return null;
                    }

                    // ✅ 关键修复：使用正确的字段名 "consumer_id" 而不是 "customer_id"
                    String customerId = safeGetString(after, "consumer_id");
                    if (customerId == null || customerId.isEmpty()) {
                        System.err.println("Consumer ID missing: " + value);
                        return null;
                    }

                    // ✅ 移除对 op="d" 的特殊处理（保留日志但不阻止写入）
                    if ("d".equals(opType)) {
                        System.out.println("Delete operation for customer: " + customerId);
                        // 这里可以添加删除HBase中对应行的逻辑
                        return "Customer deleted: " + customerId;
                    }

                    Put put = new Put(Bytes.toBytes(customerId));
                    addColumnIfNotNull(put, "info", "register_time", after);
                    addColumnIfNotNull(put, "info", "is_88vip", after);
                    addColumnIfNotNull(put, "info", "first_shopping_time", after);
                    addColumnIfNotNull(put, "info", "history_shop_cnt", after);

                    table.put(put);
                    return "Customer written to HBase: " + customerId;

                } catch (Exception e) {
                    System.err.println("Failed to process customer info: " + value);
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }

            private String safeGetString(JSONObject obj, String key) {
                if (obj == null) return null;
                Object val = obj.get(key);
                return val == null ? null : val.toString();
            }

            private void addColumnIfNotNull(Put put, String family, String qualifier, JSONObject obj) {
                String value = safeGetString(obj, qualifier);
                if (value != null && !value.isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
        }).print().name("Customer Info HBase Sink");

        // ==================== 3. Goods Info ====================
        goodsInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table table;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                table = hbaseConn.getTable(TableName.valueOf("dim_goods"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");
                    String opType = json.getString("op");

                    if (after == null) {
                        System.err.println("After is null: " + value);
                        return null;
                    }

                    String goodsId = safeGetString(after, "goods_id");
                    if (goodsId == null || goodsId.isEmpty()) {
                        System.err.println("Goods ID missing: " + value);
                        return null;
                    }

                    if ("d".equals(opType)) {
                        System.out.println("Delete operation for goods: " + goodsId);
                        return "Goods deleted: " + goodsId;
                    }

                    Put put = new Put(Bytes.toBytes(goodsId));
                    addColumnIfNotNull(put, "info", "goods_name", after);
                    addColumnIfNotNull(put, "info", "sku_list", after);
                    addColumnIfNotNull(put, "info", "goods_category", after);
                    addColumnIfNotNull(put, "info", "shelf_time", after);
                    addColumnIfNotNull(put, "info", "off_shelf_time", after);
                    addColumnIfNotNull(put, "info", "is_pre_sale", after);
                    addColumnIfNotNull(put, "info", "is_support_pay_later", after);

                    table.put(put);
                    return "Goods written to HBase: " + goodsId;

                } catch (Exception e) {
                    System.err.println("Failed to process goods info: " + value);
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }

            private String safeGetString(JSONObject obj, String key) {
                if (obj == null) return null;
                Object val = obj.get(key);
                return val == null ? null : val.toString();
            }

            private void addColumnIfNotNull(Put put, String family, String qualifier, JSONObject obj) {
                String value = safeGetString(obj, qualifier);
                if (value != null && !value.isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
        }).print().name("Goods Info HBase Sink");

        // ==================== 4. Membership Info ====================
        membershipInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table table;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                table = hbaseConn.getTable(TableName.valueOf("dim_membership"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");
                    String opType = json.getString("op");

                    if (after == null) {
                        System.err.println("After is null: " + value);
                        return null;
                    }

                    String membershipId = safeGetString(after, "membership_id");
                    if (membershipId == null || membershipId.isEmpty()) {
                        System.err.println("Membership ID missing: " + value);
                        return null;
                    }

                    if ("d".equals(opType)) {
                        System.out.println("Delete operation for membership: " + membershipId);
                        return "Membership deleted: " + membershipId;
                    }

                    Put put = new Put(Bytes.toBytes(membershipId));
                    addColumnIfNotNull(put, "info", "consumer_id", after);
                    addColumnIfNotNull(put, "info", "shop_id", after);
                    addColumnIfNotNull(put, "info", "join_time", after);
                    addColumnIfNotNull(put, "info", "membership_type", after);
                    addColumnIfNotNull(put, "info", "is_quit", after);

                    table.put(put);
                    return "Membership written to HBase: " + membershipId;

                } catch (Exception e) {
                    System.err.println("Failed to process membership info: " + value);
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }

            private String safeGetString(JSONObject obj, String key) {
                if (obj == null) return null;
                Object val = obj.get(key);
                return val == null ? null : val.toString();
            }

            private void addColumnIfNotNull(Put put, String family, String qualifier, JSONObject obj) {
                String value = safeGetString(obj, qualifier);
                if (value != null && !value.isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
        }).print().name("Membership Info HBase Sink");

        // ==================== 5. Visitor Info ====================
        visitorInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table table;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                table = hbaseConn.getTable(TableName.valueOf("dim_visitor"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");
                    String opType = json.getString("op");

                    if (after == null) {
                        System.err.println("After is null: " + value);
                        return null;
                    }

                    String visitorId = safeGetString(after, "visitor_id");
                    if (visitorId == null || visitorId.isEmpty()) {
                        System.err.println("Visitor ID missing: " + value);
                        return null;
                    }

                    if ("d".equals(opType)) {
                        System.out.println("Delete operation for visitor: " + visitorId);
                        return "Visitor deleted: " + visitorId;
                    }

                    Put put = new Put(Bytes.toBytes(visitorId));
                    addColumnIfNotNull(put, "info", "consumer_id", after);
                    addColumnIfNotNull(put, "info", "shop_id", after);
                    addColumnIfNotNull(put, "info", "goods_id", after);
                    addColumnIfNotNull(put, "info", "behavior_time", after);
                    addColumnIfNotNull(put, "info", "channel", after);
                    addColumnIfNotNull(put, "info", "device_type", after);
                    addColumnIfNotNull(put, "info", "visit_page_type", after);
                    addColumnIfNotNull(put, "info", "is_new_visitor", after);
                    addColumnIfNotNull(put, "info", "user_agent", after);

                    table.put(put);
                    return "Visitor written to HBase: " + visitorId;

                } catch (Exception e) {
                    System.err.println("Failed to process visitor info: " + value);
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }

            private String safeGetString(JSONObject obj, String key) {
                if (obj == null) return null;
                Object val = obj.get(key);
                return val == null ? null : val.toString();
            }

            private void addColumnIfNotNull(Put put, String family, String qualifier, JSONObject obj) {
                String value = safeGetString(obj, qualifier);
                if (value != null && !value.isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
        }).print().name("Visitor Info HBase Sink");

        // ==================== 6. Channel Config Info ====================
        channelConfigInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table table;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                table = hbaseConn.getTable(TableName.valueOf("dim_channel"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");
                    String opType = json.getString("op");

                    if (after == null) {
                        System.err.println("After is null: " + value);
                        return null;
                    }

                    String channelId = safeGetString(after, "channel_code");
                    if (channelId == null || channelId.isEmpty()) {
                        System.err.println("Channel ID missing: " + value);
                        return null;
                    }

                    if ("d".equals(opType)) {
                        System.out.println("Delete operation for channel: " + channelId);
                        return "Channel deleted: " + channelId;
                    }

                    Put put = new Put(Bytes.toBytes(channelId));
                    addColumnIfNotNull(put, "info", "channel_name", after);
                    addColumnIfNotNull(put, "info", "channel_type", after);
                    addColumnIfNotNull(put, "info", "is_visible_in_report", after);

                    table.put(put);
                    return "Channel written to HBase: " + channelId;

                } catch (Exception e) {
                    System.err.println("Failed to process channel config info: " + value);
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }

            private String safeGetString(JSONObject obj, String key) {
                if (obj == null) return null;
                Object val = obj.get(key);
                return val == null ? null : val.toString();
            }

            private void addColumnIfNotNull(Put put, String family, String qualifier, JSONObject obj) {
                String value = safeGetString(obj, qualifier);
                if (value != null && !value.isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
        }).print().name("Channel Config Info HBase Sink");

        env.execute("Dimension Data to HBase App");
    }

    // 创建 Kafka Source 的辅助方法
    private static KafkaSource<String> createKafkaSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();
    }
}