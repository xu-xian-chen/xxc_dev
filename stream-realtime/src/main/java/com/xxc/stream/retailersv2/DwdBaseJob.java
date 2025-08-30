package com.xxc.stream.retailersv2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DwdBaseJob {

    private static final String BOOTSTRAP_SERVERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // =========== 1. Source ===========
        KafkaSource<String> odsSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics(
                        "ods_behavior_log",
                        "ods_mysql_ods_order_detail",
                        "ods_mysql_ods_order_info",
                        "ods_mysql_ods_pay_detail",
                        "ods_mysql_ods_refund_info"
                )
                .setGroupId("dwd_base_job_" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> odsStream = env.fromSource(odsSource, WatermarkStrategy.noWatermarks(), "ods_source");

        // =========== 2. Process ===========
        odsStream
                .map(json -> {
                    try {
                        JSONObject obj = JSON.parseObject(json);
                        String table = obj.getString("__table");
                        obj.put("dt", formatDt(obj.getLong("ts_ms"))); // 加 dt 字段
                        return obj.toJSONString();
                    } catch (Exception e) {
                        return json;
                    }
                })
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<String>builder()
                                        .setTopicSelector(DwdBaseJob::routeDwdTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build()
                ).name("sink_dwd");

        env.execute("dwd_base_job");
    }

    // 动态路由到 dwd 主题
    private static String routeDwdTopic(String json) {
        try {
            JSONObject obj = JSON.parseObject(json);
            String table = obj.getString("__table");
            if (table == null) {
                return "dwd_unknown";
            }
            switch (table) {
                case "user_info":
                    return "dwd_user_info";
                case "order_info":
                    return "dwd_order_info";
                case "order_detail":
                    return "dwd_order_detail";
                case "pay_detail":
                    return "dwd_pay_detail";
                case "refund_info":
                    return "dwd_refund_info";
                default:
                    return "dwd_behavior_log"; // 兜底行为日志
            }
        } catch (Exception e) {
            return "dwd_unknown";
        }
    }

    private static String formatDt(Long ts) {
        if (ts == null) {
            return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        }
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts));
    }
}
