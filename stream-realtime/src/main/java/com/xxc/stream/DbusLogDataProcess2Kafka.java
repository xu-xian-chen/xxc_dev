package com.xxc.stream;

import com.xxc.utils.CommonUtils;
import com.xxc.utils.ConfigUtils;
import com.xxc.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.json.JSONObject;

import java.util.Date;
import java.util.HashMap;

/**
 * Title: rtest
 * Author xuxianchen
 * Package com.xxc.stream
 * Date 2025/8/15 15:15
 * description: read kafka log to split
 */

public class DbusLogDataProcess2Kafka {

    private static final String KAFKA_TOPIC_BASE_LOG_DATA = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_ERR_LOG = ConfigUtils.getString("kafka.err.log");
    private static final String KAFKA_START_LOG = ConfigUtils.getString("kafka.start.log");
    private static final String KAFKA_DISPLAY_LOG = ConfigUtils.getString("kafka.display.log");
    private static final String KAFKA_ACTION_LOG = ConfigUtils.getString("kafka.action.log");
    private static final String KAFKA_DIRTY_TOPIC = ConfigUtils.getString("kafka.dirty.topic");
    private static final String KAFKA_PAGE_TOPIC = ConfigUtils.getString("kafka.page.topic");

    private static final OutputTag<String> ERR_TAG = new OutputTag<String>("errTag") {};
    private static final OutputTag<String> START_TAG = new OutputTag<String>("startTag") {};
    private static final OutputTag<String> DISPLAY_TAG = new OutputTag<String>("displayTag") {};
    private static final OutputTag<String> ACTION_TAG = new OutputTag<String>("actionTag") {};
    private static final OutputTag<String> PAGE_TAG = new OutputTag<String>("pageTag") {};
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<String>("dirtyTag") {};
    private static final HashMap<String,DataStream<String>> COLLECTDS_MAP = new HashMap<>();

    @SneakyThrows
    public static void main(String[] args)  {

        CommonUtils.printCheckPropEnv(
                false,
                KAFKA_TOPIC_BASE_LOG_DATA,
                KAFKA_BOTSTRAP_SERVERS,
                KAFKA_PAGE_TOPIC,
                KAFKA_ERR_LOG,
                KAFKA_START_LOG,
                KAFKA_DISPLAY_LOG,
                KAFKA_ACTION_LOG,
                KAFKA_DIRTY_TOPIC
        );

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSecureSource(
                KAFKA_BOTSTRAP_SERVERS,
                KAFKA_TOPIC_BASE_LOG_DATA,
                new Date().toString(),
                OffsetsInitializer.earliest()
        );

        DataStream<String> kafkaSourceDs = env.fromSource(
                kafkaSource,
                // 如果有事件时间，换成实际的水位线策略
                WatermarkStrategy.noWatermarks(),
                "kafka_source"
        );

        SingleOutputStreamOperator<JSONObject> processJsonDs = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                            JSONObject jsonObject = new JSONObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            context.output(DIRTY_TAG, s);
                            System.err.println("conversion JSON error "+s);
                        }
                    }
                }).uid("conversion_json_process")
                .name("conversion_json_process");

        SideOutputDataStream<String> dirtyTag = processJsonDs.getSideOutput(DIRTY_TAG);
        dirtyTag.print("dirtyTag  ->");
        dirtyTag.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS, KAFKA_ERR_LOG))
                .uid("sink_dirty_data_to_kafka")
                .name("sink_dirty_data_to_kafka");

        processJsonDs.print("processJsonDs ->");

        KeyedStream<JSONObject, String> KeyedMidStream = processJsonDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        KeyedMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                return null;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });


        env.execute();
    }
}
