package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.ConfigUtils;
import com.xxc.utils.EnvironmentSettingUtils;
import com.xxc.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Date;

public class DwdDbRouterApp {

    // === 配置键名与现有工程保持一致 ===
    private static final String BOOTSTRAP     = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String SRC_TOPIC     = ConfigUtils.getString("kafka.cdc.db.topic");

    private static final String T_ORDER_INFO   = ConfigUtils.getString("kafka.dwd.trade.order_info");
    private static final String T_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order_detail");
    private static final String T_PAYMENT_INFO = ConfigUtils.getString("kafka.dwd.trade.payment_info");
    private static final String T_REFUND_INFO  = ConfigUtils.getString("kafka.dwd.trade.refund_info");
    private static final String T_CART_ADD     = ConfigUtils.getString("kafka.dwd.trade.cart_add");
    private static final String T_FAVOR_ADD    = ConfigUtils.getString("kafka.dwd.trade.favor_add");
    private static final String T_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.trade.comment_info");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // ---- 正确的 Source 写法：使用你封装的 KafkaSource ----
        KafkaSource<String> source = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP,
                SRC_TOPIC,
                new Date().toString(),                // groupId：你工程里常用这种占位
                OffsetsInitializer.earliest()
        ); // 见 KafkaUtils.buildKafkaSecureSource 定义

        DataStream<String> src = env.fromSource(
                source,
                // Debezium 走事件时间：用 ts_ms 赋时（和你评论作业一致）
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, ts) -> {
                            try { return JSON.parseObject(event).getLongValue("ts_ms"); }
                            catch (Exception e) { return 0L; }
                        }),
                "kafka-ods-db"
        );

        SingleOutputStreamOperator<JSONObject> normalized = src.map(new DebeziumNormalizer());

        normalized.filter(byTable("order_info"))
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_ORDER_INFO))
                .name("dwd-order-info");

        normalized.filter(byTable("order_detail"))
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_ORDER_DETAIL))
                .name("dwd-order-detail");

        normalized.filter(byTable("payment_info"))
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_PAYMENT_INFO))
                .name("dwd-payment-info");

        normalized.filter(byTable("order_refund"))
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_REFUND_INFO))
                .name("dwd-refund-info");

        // 行为型：通常只保留新增
        normalized.filter(new FilterFunction<JSONObject>() {
                    @Override public boolean filter(JSONObject rec) {
                        return "cart_info".equals(rec.getString("_table")) && "c".equals(rec.getString("op"));
                    }
                })
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_CART_ADD))
                .name("dwd-cart-add");

        normalized.filter(new FilterFunction<JSONObject>() {
                    @Override public boolean filter(JSONObject rec) {
                        return "favor_info".equals(rec.getString("_table")) && "c".equals(rec.getString("op"));
                    }
                })
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_FAVOR_ADD))
                .name("dwd-favor-add");

        normalized.filter(byTable("comment_info"))
                .map(stripMetaToString())
                .sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_COMMENT_INFO))
                .name("dwd-comment-info");

        env.execute("DWD DB Router App");
    }

    // ---------- helpers ----------
    static class DebeziumNormalizer implements MapFunction<String, JSONObject> {
        @Override public JSONObject map(String value) {
            JSONObject out = new JSONObject(true);
            try {
                JSONObject root = JSON.parseObject(value);
                if (root == null) {
                    return out;
                }
                String op = root.getString("op");                  // c/u/d/r
                JSONObject source = root.getJSONObject("source");
                String table = source == null ? null : source.getString("table");
                Long tsMs = root.getLong("ts_ms");
                JSONObject payload = "d".equals(op) ? root.getJSONObject("before") : root.getJSONObject("after");
                if (payload != null) {
                    for (String k : payload.keySet()) {
                        out.put(k, payload.get(k));
                    }
                }
                out.put("_table", table);
                out.put("op", "r".equals(op) ? "c" : op);
                out.put("event_time", tsMs);
            } catch (Exception ignore) {}
            return out;
        }
    }

    private static FilterFunction<JSONObject> byTable(final String table) {
        return new FilterFunction<JSONObject>() {
            @Override public boolean filter(JSONObject rec) {
                return table.equals(rec.getString("_table"));
            }
        };
    }

    private static MapFunction<JSONObject, String> stripMetaToString() {
        return new MapFunction<JSONObject, String>() {
            @Override public String map(JSONObject rec) {
                rec.remove("_table");
                return rec.toJSONString();
            }
        };
    }
}
