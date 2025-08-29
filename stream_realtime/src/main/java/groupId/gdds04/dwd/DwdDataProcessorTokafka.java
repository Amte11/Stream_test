package groupId.gdds04.dwd;

import com.alibaba.fastjson.JSONObject;
import com.utils.ConfigUtils;
import com.utils.EnvironmentSettingUtils;
import com.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * Title: DwdDataProcessorTokafka
 * Author: hyx
 * Package: groupId.gdds04.dwd
 * Date: 2025/8/29 18:50
 * Description: DWD层数据处理：从ODS层Kafka主题读取数据，处理后写入DWD层Kafka主题
 */
public class DwdDataProcessorTokafka {

    private static final String DWD_VISITOR_INFO_TOPIC = "dwd_visitor_info_detail";
    private static final String DWD_CART_INFO_TOPIC = "dwd_cart_info_detail";
    private static final String DWD_PAYMENT_INFO_TOPIC = "dwd_payment_info_detail";
    private static final String DWD_SEARCH_INFO_TOPIC = "dwd_search_info_detail";
    private static final String DWD_COLLECT_INFO_TOPIC = "dwd_collect_info_detail";
    private static final String DWD_MEMBERSHIP_INFO_TOPIC = "dwd_membership_info_detail";
    private static final String DWD_CONSULT_INFO_TOPIC = "dwd_consult_info_detail";
    private static final String DWD_POPULARITY_BROADCAST_TOPIC = "dwd_popularity_broadcastinfo_detail";
    private static final String DWD_SHOP_INFO_TOPIC = "dwd_shop_info";
    private static final String DWD_GOODS_INFO_TOPIC = "dwd_goods_info";
    private static final String DWD_CUSTOMER_INFO_TOPIC = "dwd_customer_info";
    private static final String DWD_CHANNEL_CONFIG_TOPIC = "dwd_channel_config_info";

    // 定义侧输出流标签
    private static final OutputTag<String> VISITOR_INFO_TAG = new OutputTag<String>("visitor-info") {};
    private static final OutputTag<String> CART_INFO_TAG = new OutputTag<String>("cart-info") {};
    private static final OutputTag<String> PAYMENT_INFO_TAG = new OutputTag<String>("payment-info") {};
    private static final OutputTag<String> SEARCH_INFO_TAG = new OutputTag<String>("search-info") {};
    private static final OutputTag<String> COLLECT_INFO_TAG = new OutputTag<String>("collect-info") {};
    private static final OutputTag<String> MEMBERSHIP_INFO_TAG = new OutputTag<String>("membership-info") {};
    private static final OutputTag<String> CONSULT_INFO_TAG = new OutputTag<String>("consult-info") {};
    private static final OutputTag<String> POPULARITY_BROADCAST_TAG = new OutputTag<String>("popularity-broadcast") {};
    private static final OutputTag<String> SHOP_INFO_TAG = new OutputTag<String>("shop-info") {};
    private static final OutputTag<String> GOODS_INFO_TAG = new OutputTag<String>("goods-info") {};
    private static final OutputTag<String> CUSTOMER_INFO_TAG = new OutputTag<String>("customer-info") {};
    private static final OutputTag<String> CHANNEL_CONFIG_TAG = new OutputTag<String>("channel-config") {};

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");

        // 读取各个ODS层主题数据
        DataStreamSource<String> visitorInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_visitor_info_detail", "dwd_visitor_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "visitor-info-source"
        );

        DataStreamSource<String> cartInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_cart_info_detail", "dwd_cart_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "cart-info-source"
        );

        DataStreamSource<String> paymentInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_payment_info_detail", "dwd_payment_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "payment-info-source"
        );

        DataStreamSource<String> searchInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_search_info_detail", "dwd_search_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "search-info-source"
        );

        DataStreamSource<String> collectInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_collect_info_detail", "dwd_collect_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "collect-info-source"
        );

        DataStreamSource<String> membershipInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_membership_info_detail", "dwd_membership_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "membership-info-source"
        );

        DataStreamSource<String> consultInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_consult_info_detail", "dwd_consult_info_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "consult-info-source"
        );

        DataStreamSource<String> popularityBroadcastStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_popularity_broadcastinfo_detail", "dwd_popularity_broadcast_detail",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "popularity-broadcast-source"
        );

        DataStreamSource<String> shopInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_shop_info", "dwd_shop_info_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "shop-info-source"
        );

        DataStreamSource<String> goodsInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_goods_info", "dwd_goods_info_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "goods-info-source"
        );

        DataStreamSource<String> customerInfoStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_customer_info", "dwd_customer_info_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "customer-info-source"
        );

        DataStreamSource<String> channelConfigStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "ods_channel_config_info", "dwd_channel_config_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "channel-config-source"
        );

        // 处理访客行为数据
        visitorInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("visitor_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("behavior_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("visitor_id", jsonObject.getString("visitor_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("behavior_time", jsonObject.getString("behavior_time"));
                            dwdData.put("channel", jsonObject.getString("channel"));
                            dwdData.put("device_type", jsonObject.getString("device_type"));
                            dwdData.put("visit_page_type", jsonObject.getString("visit_page_type"));
                            dwdData.put("is_new_visitor", jsonObject.getInteger("is_new_visitor"));
                            dwdData.put("user_agent", jsonObject.getString("user_agent"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(VISITOR_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理访客行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(VISITOR_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_VISITOR_INFO_TOPIC))
                .name("sink-visitor-info-dwd");

        // 处理加购行为数据
        cartInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("addcart_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("addcart_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("addcart_id", jsonObject.getString("addcart_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("sku_id", jsonObject.getString("sku_id"));
                            dwdData.put("addcart_time", jsonObject.getString("addcart_time"));
                            dwdData.put("channel", jsonObject.getString("channel"));
                            dwdData.put("addcart_num", jsonObject.getInteger("addcart_num"));
                            dwdData.put("is_cancel_addcart", jsonObject.getInteger("is_cancel_addcart"));
                            dwdData.put("history_addcart_cnt", jsonObject.getInteger("history_addcart_cnt"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(CART_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理加购行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(CART_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_CART_INFO_TOPIC))
                .name("sink-cart-info-dwd");

        // 处理支付行为数据
        paymentInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("payment_id") == null ||
                                    jsonObject.getString("order_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("payment_id", jsonObject.getString("payment_id"));
                            dwdData.put("order_id", jsonObject.getString("order_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("sku_id", jsonObject.getString("sku_id"));
                            dwdData.put("payment_date", jsonObject.getString("payment_date"));
                            dwdData.put("payment_time", jsonObject.getString("payment_time"));
                            dwdData.put("order_type", jsonObject.getString("order_type"));
                            dwdData.put("deposit_amt", jsonObject.getBigDecimal("deposit_amt"));
                            dwdData.put("balance_amt", jsonObject.getBigDecimal("balance_amt"));
                            dwdData.put("total_pay_amt", jsonObject.getBigDecimal("total_pay_amt"));
                            dwdData.put("freight_amt", jsonObject.getBigDecimal("freight_amt"));
                            dwdData.put("shopping_fund_amt", jsonObject.getBigDecimal("shopping_fund_amt"));
                            dwdData.put("red_packet_amt", jsonObject.getBigDecimal("red_packet_amt"));
                            dwdData.put("is_taote", jsonObject.getInteger("is_taote"));
                            dwdData.put("is_cross_border", jsonObject.getInteger("is_cross_border"));
                            dwdData.put("is_subsidy_half_hosting", jsonObject.getInteger("is_subsidy_half_hosting"));
                            dwdData.put("is_official_bid", jsonObject.getInteger("is_official_bid"));
                            dwdData.put("coupon_amt", jsonObject.getBigDecimal("coupon_amt"));
                            dwdData.put("is_small_pay", jsonObject.getInteger("is_small_pay"));
                            dwdData.put("refund_mid_sale", jsonObject.getBigDecimal("refund_mid_sale"));
                            dwdData.put("refund_after_sale", jsonObject.getBigDecimal("refund_after_sale"));
                            dwdData.put("is_instant_refund", jsonObject.getInteger("is_instant_refund"));
                            dwdData.put("is_balance_paid", jsonObject.getInteger("is_balance_paid"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(PAYMENT_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理支付行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(PAYMENT_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_PAYMENT_INFO_TOPIC))
                .name("sink-payment-info-dwd");

        // 处理搜索行为数据
        searchInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("search_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("search_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("search_id", jsonObject.getString("search_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("search_keyword", jsonObject.getString("search_keyword"));
                            dwdData.put("click_goods_id", jsonObject.getString("click_goods_id"));
                            dwdData.put("search_time", jsonObject.getString("search_time"));
                            dwdData.put("search_stay_sec", jsonObject.getInteger("search_stay_sec"));
                            dwdData.put("channel", jsonObject.getString("channel"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(SEARCH_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理搜索行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(SEARCH_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_SEARCH_INFO_TOPIC))
                .name("sink-search-info-dwd");

        // 处理收藏行为数据
        collectInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("collect_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("collect_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("collect_id", jsonObject.getString("collect_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("collect_time", jsonObject.getString("collect_time"));
                            dwdData.put("is_cancel_collect", jsonObject.getInteger("is_cancel_collect"));
                            dwdData.put("history_collect_cnt", jsonObject.getInteger("history_collect_cnt"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(COLLECT_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理收藏行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(COLLECT_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_COLLECT_INFO_TOPIC))
                .name("sink-collect-info-dwd");

        // 处理入会行为数据
        membershipInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("membership_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("join_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("membership_id", jsonObject.getString("membership_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("join_time", jsonObject.getString("join_time"));
                            dwdData.put("membership_type", jsonObject.getString("membership_type"));
                            dwdData.put("is_quit", jsonObject.getInteger("is_quit"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(MEMBERSHIP_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理入会行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(MEMBERSHIP_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_MEMBERSHIP_INFO_TOPIC))
                .name("sink-membership-info-dwd");

        // 处理咨询行为数据
        consultInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("consult_id") == null ||
                                    jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("consult_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("consult_id", jsonObject.getString("consult_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("consult_time", jsonObject.getString("consult_time"));
                            dwdData.put("consult_content", jsonObject.getString("consult_content"));
                            dwdData.put("is_replied", jsonObject.getInteger("is_replied"));
                            dwdData.put("reply_time", jsonObject.getString("reply_time"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(CONSULT_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理咨询行为数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(CONSULT_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_CONSULT_INFO_TOPIC))
                .name("sink-consult-info-dwd");

        // 处理人气播报数据
        popularityBroadcastStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("broadcast_id") == null ||
                                    jsonObject.getString("broadcast_time") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("consumer_id") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("broadcast_id", jsonObject.getString("broadcast_id"));
                            dwdData.put("broadcast_time", jsonObject.getString("broadcast_time"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("consumer_type", jsonObject.getString("consumer_type"));
                            dwdData.put("behavior_type", jsonObject.getString("behavior_type"));
                            dwdData.put("channel", jsonObject.getString("channel"));
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("history_behavior_cnt", jsonObject.getInteger("history_behavior_cnt"));
                            dwdData.put("broadcast_content", jsonObject.getString("broadcast_content"));
                            dwdData.put("is_triggered", jsonObject.getInteger("is_triggered"));
                            dwdData.put("related_behavior_ids", jsonObject.getString("related_behavior_ids"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(POPULARITY_BROADCAST_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理人气播报数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(POPULARITY_BROADCAST_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_POPULARITY_BROADCAST_TOPIC))
                .name("sink-popularity-broadcast-dwd");

        // 处理店铺基础信息数据
        shopInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("merchant_id") == null ||
                                    jsonObject.getString("shop_name") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("merchant_id", jsonObject.getString("merchant_id"));
                            dwdData.put("shop_name", jsonObject.getString("shop_name"));
                            dwdData.put("shop_category", jsonObject.getString("shop_category"));
                            dwdData.put("open_date", jsonObject.getString("open_date"));
                            dwdData.put("is_direct_bus", jsonObject.getInteger("is_direct_bus"));
                            dwdData.put("business_scope", jsonObject.getString("business_scope"));
                            dwdData.put("create_time", jsonObject.getString("create_time"));
                            dwdData.put("update_time", jsonObject.getString("update_time"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(SHOP_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理店铺基础信息数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(SHOP_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_SHOP_INFO_TOPIC))
                .name("sink-shop-info-dwd");

        // 处理商品基础信息数据
        goodsInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("goods_id") == null ||
                                    jsonObject.getString("shop_id") == null ||
                                    jsonObject.getString("goods_name") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("goods_id", jsonObject.getString("goods_id"));
                            dwdData.put("shop_id", jsonObject.getString("shop_id"));
                            dwdData.put("goods_name", jsonObject.getString("goods_name"));
                            dwdData.put("sku_list", jsonObject.getString("sku_list"));
                            dwdData.put("goods_category", jsonObject.getString("goods_category"));
                            dwdData.put("shelf_time", jsonObject.getString("shelf_time"));
                            dwdData.put("off_shelf_time", jsonObject.getString("off_shelf_time"));
                            dwdData.put("is_pre_sale", jsonObject.getInteger("is_pre_sale"));
                            dwdData.put("is_support_pay_later", jsonObject.getInteger("is_support_pay_later"));
                            dwdData.put("create_time", jsonObject.getString("create_time"));
                            dwdData.put("update_time", jsonObject.getString("update_time"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(GOODS_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理商品基础信息数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(GOODS_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_GOODS_INFO_TOPIC))
                .name("sink-goods-info-dwd");

        // 处理消费者基础信息数据
        customerInfoStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("consumer_id") == null ||
                                    jsonObject.getString("register_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("consumer_id", jsonObject.getString("consumer_id"));
                            dwdData.put("register_time", jsonObject.getString("register_time"));
                            dwdData.put("is_88vip", jsonObject.getInteger("is_88vip"));
                            dwdData.put("first_shopping_time", jsonObject.getString("first_shopping_time"));
                            dwdData.put("history_shop_cnt", jsonObject.getInteger("history_shop_cnt"));
                            dwdData.put("create_time", jsonObject.getString("create_time"));
                            dwdData.put("update_time", jsonObject.getString("update_time"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(CUSTOMER_INFO_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理消费者基础信息数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(CUSTOMER_INFO_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_CUSTOMER_INFO_TOPIC))
                .name("sink-customer-info-dwd");

        // 处理渠道配置数据
        channelConfigStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗和验证
                            if (jsonObject.getString("channel_code") == null ||
                                    jsonObject.getString("channel_name") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("channel_code", jsonObject.getString("channel_code"));
                            dwdData.put("channel_name", jsonObject.getString("channel_name"));
                            dwdData.put("channel_type", jsonObject.getString("channel_type"));
                            dwdData.put("is_visible_in_report", jsonObject.getString("is_visible_in_report"));

                            // 添加处理时间戳
                            dwdData.put("process_time", System.currentTimeMillis());

                            ctx.output(CHANNEL_CONFIG_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理渠道配置数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(CHANNEL_CONFIG_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_CHANNEL_CONFIG_TOPIC))
                .name("sink-channel-config-dwd");

        env.execute("DWD Layer Data Processing");
    }
}
