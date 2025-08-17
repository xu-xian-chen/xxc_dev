package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.xxc.stream.retailersv1.func.ProcessSplitStreamFunc;
import com.xxc.utils.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
                            JSONObject jsonObject = JSONObject.parseObject(s);
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

        SingleOutputStreamOperator<JSONObject> mapDs = KeyedMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastStartTime;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastStartTime", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastStartTime = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                long ts = jsonObject.getLong("ts");
                String date = DateTimeUtils.tsToDate(ts);
                String lastTime = lastStartTime.value();
                if ("1".equals(isNew)) {
                    //标记为新用户
                    if (StringsUtils.isEmpty(lastTime)) {
                        //第一次启动
                        lastStartTime.update(date);
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    } else {
                        if (!lastTime.equals(date)) {
                            //不是同一天
                            lastStartTime.update(date);
                            jsonObject.getJSONObject("common").put("is_new", "0");
                        }
                    }
                } else {
                    //标记为老用户
                    if (StringsUtils.isEmpty(lastTime)) {
                        jsonObject.getJSONObject("common").put("is_new", "1");
                    } else {
                        if (!lastTime.equals(date)) {
                            //不是同天
                            lastStartTime.update(date);
                        }
                    }
                }
                return jsonObject;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }).name("map_is_new_process");

        mapDs.print("mapDs  ->");

        SingleOutputStreamOperator<String> process = mapDs.process(
                new ProcessSplitStreamFunc(ERR_TAG, START_TAG, DISPLAY_TAG, ACTION_TAG)
        );

        SideOutputDataStream<String> errStream = process.getSideOutput(ERR_TAG);
        SideOutputDataStream<String> startStream = process.getSideOutput(START_TAG);
        SideOutputDataStream<String> displayStream = process.getSideOutput(DISPLAY_TAG);
        SideOutputDataStream<String> actionStream = process.getSideOutput(ACTION_TAG);

        errStream.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS, KAFKA_ERR_LOG))
                .uid("sink_err_data_to_kafka")
                .name("sink_err_data_to_kafka");

        startStream.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS, KAFKA_START_LOG))
                .uid("sink_start_data_to_kafka")
                .name("sink_start_data_to_kafka");

        displayStream.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS, KAFKA_DISPLAY_LOG))
                .uid("sink_display_data_to_kafka")
                .name("sink_display_data_to_kafka");

        actionStream.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS, KAFKA_ACTION_LOG))
                .uid("sink_action_data_to_kafka")
                .name("sink_action_data_to_kafka");

        process.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS, KAFKA_PAGE_TOPIC))
                .uid("sink_page_data_to_kafka")
                .name("sink_page_data_to_kafka");

        process.print("process ->");
        errStream.print("err  ->");
        startStream.print("start  ->");
        displayStream.print("display  ->");
        actionStream.print("action  ->");


        env.execute();
    }
}
