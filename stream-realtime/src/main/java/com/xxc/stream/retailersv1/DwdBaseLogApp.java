package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Optional;
import java.util.Properties;

/**
 * DWD 基础日志层
 * 输入：ODS(base_log) 每行一条完整 JSON
 * 输出：dwd_log_start / dwd_log_page / dwd_log_display / dwd_log_action / dwd_log_error
 *
 * 约定字段：
 *  - 公共：mid, uid, vc, ch, ar, ba, md, os, is_new
 *  - 统一时间字段：event_time (long，来自最外层 ts 或 action 内部 ts)
 */
public class DwdBaseLogApp {

    // ========= 可按需修改（或改成读取配置） =========
    private static final String BOOTSTRAP = Optional.ofNullable(System.getProperty("kafka.bootstrap"))
            .orElse("localhost:9092");
    private static final String GROUP_ID = Optional.ofNullable(System.getProperty("kafka.group"))
            .orElse("dwd_base_log_group");
    private static final String SRC_TOPIC = Optional.ofNullable(System.getProperty("kafka.src.topic"))
            .orElse("ods_base_log");

    private static final String T_START   = Optional.ofNullable(System.getProperty("kafka.dwd.start")).orElse("dwd_log_start");
    private static final String T_PAGE    = Optional.ofNullable(System.getProperty("kafka.dwd.page")).orElse("dwd_log_page");
    private static final String T_DISPLAY = Optional.ofNullable(System.getProperty("kafka.dwd.display")).orElse("dwd_log_display");
    private static final String T_ACTION  = Optional.ofNullable(System.getProperty("kafka.dwd.action")).orElse("dwd_log_action");
    private static final String T_ERROR   = Optional.ofNullable(System.getProperty("kafka.dwd.error")).orElse("dwd_log_error");
    // ===========================================

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L); // 可按需调整
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // Source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP);
        props.setProperty("group.id", GROUP_ID);
        props.setProperty("auto.offset.reset", "latest");
        DataStream<String> src = env.addSource(new FlinkKafkaConsumer<>(SRC_TOPIC, new SimpleStringSchema(), props));

        // 分流侧输出
        OutputTag<String> TAG_START   = new OutputTag<String>("start"){};
        OutputTag<String> TAG_PAGE    = new OutputTag<String>("page"){};
        OutputTag<String> TAG_DISPLAY = new OutputTag<String>("display"){};
        OutputTag<String> TAG_ACTION  = new OutputTag<String>("action"){};
        OutputTag<String> TAG_ERROR   = new OutputTag<String>("error"){};

        // 统一分发器：解析 JSON，输出到各自侧输出
        SingleOutputStreamOperator<String> main = src.process(new org.apache.flink.streaming.api.functions.ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) {
                JSONObject root;
                try {
                    root = JSON.parseObject(value);
                } catch (Exception e) {
                    // 无法解析，送 error
                    ctx.output(TAG_ERROR, buildErrorRow(null, null, value, System.currentTimeMillis()));
                    return;
                }

                JSONObject common = root.getJSONObject("common");
                Long ts = root.getLong("ts");

                // start
                if (root.containsKey("start")) {
                    JSONObject start = root.getJSONObject("start");
                    JSONObject row = baseCommon(common)
                            .fluentPut("entry", start.getString("entry"))
                            .fluentPut("loading_time", start.getLongValue("loading_time"))
                            .fluentPut("open_ad_id", start.getInteger("open_ad_id"))
                            .fluentPut("open_ad_ms", start.getLongValue("open_ad_ms"))
                            .fluentPut("open_ad_skip_ms", start.getLongValue("open_ad_skip_ms"))
                            .fluentPut("event_time", ts);
                    ctx.output(TAG_START, row.toJSONString());
                }

                // page
                if (root.containsKey("page")) {
                    JSONObject page = root.getJSONObject("page");
                    JSONObject row = baseCommon(common)
                            .fluentPut("page_id", page.getString("page_id"))
                            .fluentPut("refer_id", page.getString("refer_id"))
                            .fluentPut("from_pos_id", page.getInteger("from_pos_id"))
                            .fluentPut("from_pos_seq", page.getInteger("from_pos_seq"))
                            .fluentPut("item", page.getString("item"))
                            .fluentPut("item_type", page.getString("item_type"))
                            .fluentPut("during_time", page.getLongValue("during_time"))
                            .fluentPut("event_time", ts);
                    ctx.output(TAG_PAGE, row.toJSONString());
                }

                // displays[]
                if (root.containsKey("displays")) {
                    JSONArray arr = root.getJSONArray("displays");
                    JSONObject page = root.getJSONObject("page");
                    String pageId = page == null ? null : page.getString("page_id");
                    if (arr != null) {
                        for (int i = 0; i < arr.size(); i++) {
                            JSONObject d = arr.getJSONObject(i);
                            JSONObject row = baseCommon(common)
                                    .fluentPut("page_id", pageId)
                                    .fluentPut("item", d.getString("item"))
                                    .fluentPut("item_type", d.getString("item_type"))
                                    .fluentPut("pos_id", d.getInteger("pos_id"))
                                    .fluentPut("pos_seq", d.getInteger("pos_seq"))
                                    .fluentPut("event_time", ts);
                            ctx.output(TAG_DISPLAY, row.toJSONString());
                        }
                    }
                }

                // actions[]
                if (root.containsKey("actions")) {
                    JSONArray arr = root.getJSONArray("actions");
                    JSONObject page = root.getJSONObject("page");
                    String pageId = page == null ? null : page.getString("page_id");
                    if (arr != null) {
                        for (int i = 0; i < arr.size(); i++) {
                            JSONObject a = arr.getJSONObject(i);
                            Long ats = a.getLong("ts");
                            JSONObject row = baseCommon(common)
                                    .fluentPut("page_id", pageId)
                                    .fluentPut("action_id", a.getString("action_id"))
                                    .fluentPut("item", a.getString("item"))
                                    .fluentPut("item_type", a.getString("item_type"))
                                    .fluentPut("event_time", ats != null ? ats : ts);
                            ctx.output(TAG_ACTION, row.toJSONString());
                        }
                    }
                }

                // err
                if (root.containsKey("err")) {
                    JSONObject err = root.getJSONObject("err");
                    ctx.output(TAG_ERROR, buildErrorRow(common, err, value, ts));
                }
            }

            private JSONObject baseCommon(JSONObject common) {
                JSONObject o = new JSONObject(true);
                if (common != null) {
                    o.fluentPut("mid", common.getString("mid"))
                            .fluentPut("uid", common.getString("uid"))
                            .fluentPut("vc", common.getString("vc"))
                            .fluentPut("ch", common.getString("ch"))
                            .fluentPut("ar", common.getString("ar"))
                            .fluentPut("ba", common.getString("ba"))
                            .fluentPut("md", common.getString("md"))
                            .fluentPut("os", common.getString("os"))
                            .fluentPut("is_new", common.getString("is_new"));
                }
                return o;
            }

            private String buildErrorRow(JSONObject common, JSONObject err, String raw, Long ts) {
                JSONObject row = new JSONObject(true);
                if (common != null) {
                    row.fluentPut("mid", common.getString("mid"))
                            .fluentPut("uid", common.getString("uid"))
                            .fluentPut("vc", common.getString("vc"));
                }
                if (err != null) {
                    row.fluentPut("error_code", err.getString("error_code"))
                       .fluentPut("msg", err.getString("msg"));
                }
                row.fluentPut("raw", raw).fluentPut("event_time", ts);
                return row.toJSONString();
            }
        });

        // 各路流
        DataStream<String> start   = main.getSideOutput(TAG_START);
        DataStream<String> page    = main.getSideOutput(TAG_PAGE);
        DataStream<String> display = main.getSideOutput(TAG_DISPLAY);
        DataStream<String> action  = main.getSideOutput(TAG_ACTION);
        DataStream<String> error   = main.getSideOutput(TAG_ERROR);

        // Sinks
        start.addSink(kafkaProducer(T_START));
        page.addSink(kafkaProducer(T_PAGE));
        display.addSink(kafkaProducer(T_DISPLAY));
        action.addSink(kafkaProducer(T_ACTION));
        error.addSink(kafkaProducer(T_ERROR));

        env.execute("DWD Base Log App");
    }

    private static FlinkKafkaProducer<String> kafkaProducer(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP);
        return new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), props);
    }
}
