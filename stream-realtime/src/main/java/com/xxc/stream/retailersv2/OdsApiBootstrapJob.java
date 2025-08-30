package com.xxc.stream.retailersv2;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.xxc.stream.retailersv2.utils.CdcSourceUtils;
import com.xxc.utils.ConfigUtils;
import com.xxc.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Title: OdsApiBootstrapJob
 * Author  xuxianchen
 * Package com.xxc.stream.retailersv2
 * Date  2025/8/27 17:04
 * description:
 */

public class OdsApiBootstrapJob {
    /**
     * 表 -> 主键名映射关系
     */
    private static final Map<String, String> TABLE_PK = new HashMap<>();
    static {
        TABLE_PK.put("ods_user_info", "user_id");
        TABLE_PK.put("ods_store_info", "store_id");
        TABLE_PK.put("ods_sku_info", "sku_id");
        TABLE_PK.put("ods_order_info", "order_id");
        TABLE_PK.put("ods_order_detail", "order_detail_id");
        TABLE_PK.put("ods_pay_detail", "pay_id");
        TABLE_PK.put("ods_refund_info", "refund_id");
        TABLE_PK.put("ods_user_profile_snapshot", "snapshot_id");
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        DataStreamSource<String> CdcSource = env.fromSource(CdcSourceUtils.getMySQLCdcSource(
                        ConfigUtils.getString("mysql.realtimeV2.database"),
                        "",
                        ConfigUtils.getString("mysql.user"),
                        ConfigUtils.getString("mysql.pwd"),
                        StartupOptions.earliest(), "52200-52299"
                ), WatermarkStrategy.noWatermarks(),
                "mysql_cdc_source"
        );
        CdcSource.print("cdc --->");

        DataStreamSource<String> kafkaSource = env.fromSource(KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        ConfigUtils.getString("kafka.topic.db"),
                        DateTime.now().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_source"
        );
        kafkaSource.print("kafka  ->>");

        // ===== 1) CDC 原始 JSON -> ods_mysql_<table>_raw =====
        CdcSource.sinkTo(
                buildDynamicKafkaSink(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        TopicRouter::cdcRaw               // 动态路由 raw 主题
                )
        ).name("sink_ods_mysql_raw");

// ===== 2) CDC after 扁平 JSON -> ods_mysql_<table> =====
        SingleOutputStreamOperator<String> cdcAfter =
                CdcSource.flatMap((String raw, org.apache.flink.util.Collector<String> out) -> {
                    String json = extractAfterJson(raw);     // 生成 only-after JSON + __op/__pk/__table
                    if (json != null) {
                        out.collect(json);
                    }
                }).returns(String.class);

        cdcAfter.sinkTo(
                buildDynamicKafkaSink(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        TopicRouter::cdcAfter            // 动态路由 after 主题
                )
        ).name("sink_ods_mysql_after");

// ===== 3) 行为日志 -> 标准化写入 ods_behavior_log =====
// 如果你的 Python 已经直接写到 ods_behavior_log，这里相当于重分区；保留可统一 ODS 出口
        kafkaSource.sinkTo(
                KafkaSink.<String>builder()
                        .setBootstrapServers(ConfigUtils.getString("kafka.bootstrap.servers"))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<String>builder()
                                        .setTopic("ods_behavior_log")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        ).build()
        ).name("sink_ods_behavior_log");


        env.execute("ods_api_bootstrap_job");
    }

    // 动态 Kafka Sink：在 serialize() 里决定 topic（兼容没有 TopicSelector 的版本）
    private static KafkaSink<String> buildDynamicKafkaSink(
            String bootstrap,
            TopicSelector<String> topicSelector
    ) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopicSelector(topicSelector)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }

    // 主题路由（原始 / after 扁平）
    private static class TopicRouter {
        // ods_mysql_<table>_raw
        static String cdcRaw(String rawJson) {
            String table = extractTable(rawJson);
            return (table == null) ? "ods_mysql_unknown_raw" : "ods_mysql_" + table + "_raw";
        }
        // ods_mysql_<table>（after JSON 中会带 __table）
        static String cdcAfter(String afterJson) {
            try {
                JSONObject obj = JSON.parseObject(afterJson);
                String table = obj.getString("__table");
                return (table == null) ? "ods_mysql_unknown" : "ods_mysql_" + table;
            } catch (Exception e) {
                return "ods_mysql_unknown";
            }
        }
    }

    // 从 Debezium JSON 提取表名（兼容顶层 / payload 包裹两种结构）
    private static String extractTable(String rawJson) {
        try {
            JSONObject obj = JSON.parseObject(rawJson);
            JSONObject source = obj.getJSONObject("source");
            if (source != null && source.getString("table") != null) {
                return source.getString("table");
            }
            JSONObject payload = obj.getJSONObject("payload");
            if (payload != null) {
                JSONObject s2 = payload.getJSONObject("source");
                if (s2 != null && s2.getString("table") != null) {
                    return s2.getString("table");
                }
            }
        } catch (Exception ignore) {}
        return null;
    }

    /** 只保留 after，并补 __op/__pk/__table；DELETE 事件返回 null */
    private static String extractAfterJson(String rawJson) {
        try {
            JSONObject obj = JSON.parseObject(rawJson);

            String op = obj.getString("op");
            JSONObject after = obj.getJSONObject("after");
            JSONObject src = obj.getJSONObject("source");

            // 兼容 payload 包裹
            if (after == null) {
                JSONObject payload = obj.getJSONObject("payload");
                if (payload != null) {
                    op = payload.getString("op");
                    after = payload.getJSONObject("after");
                    src = payload.getJSONObject("source");
                }
            }
            if (op == null || "d".equalsIgnoreCase(op) || after == null) {
                return null;
            }

            String table = (src == null) ? null : src.getString("table");
            after.put("__op", op);
            after.put("__table", table);

            if (table != null) {
                String pkField = TABLE_PK.get(table);     // 直接使用你上面定义的主键映射
                if (pkField != null && after.containsKey(pkField)) {
                    after.put("__pk", after.getString(pkField));
                }
            }
            return after.toJSONString();
        } catch (Exception e) {
            return null;
        }
    }


}
