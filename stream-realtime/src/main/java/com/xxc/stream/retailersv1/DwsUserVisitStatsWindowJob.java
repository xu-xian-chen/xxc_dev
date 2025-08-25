package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.ConfigUtils;
import com.xxc.utils.EnvironmentSettingUtils;
import com.xxc.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class DwsUserVisitStatsWindowJob {


    private static long tsOf(JSONObject j){
        // 日志里通常是 ts（毫秒）
        long ts = j.getLongValue("ts");
        if (ts == 0L) {
            ts = j.getLongValue("event_time");
        }
        return ts == 0L ? System.currentTimeMillis() : ts;
    }
    private static String visitorIdOf(JSONObject j){
        // 优先 uid，没有则用 mid（设备）
        JSONObject common = j.getJSONObject("common");
        if (common != null) {
            String uid = common.getString("uid");
            if (uid != null && !uid.isEmpty() && !"null".equalsIgnoreCase(uid)) {
                return uid;
            }
            String mid = common.getString("mid");
            if (mid != null && !mid.isEmpty()) {
                return mid;
            }
        }
        // 兜底
        return j.getString("uid") != null ? j.getString("uid") : j.getString("mid");
    }
    private static String sidOf(JSONObject j){
        JSONObject common = j.getJSONObject("common");
        if (common != null) {
            return common.getString("sid");
        }
        return j.getString("sid");
    }
    private static long pageDurationOf(JSONObject j){
        JSONObject page = j.getJSONObject("page");
        return page == null ? 0L : page.getLongValue("during_time");
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        final String BOOTSTRAP = ConfigUtils.getString("kafka.bootstrap.servers");
        final String SRC_PAGE  = ConfigUtils.getString("kafka.page.topic");
        final String SRC_START = ConfigUtils.getString("kafka.start.log");
        final String OUT_TOPIC = ConfigUtils.getString("kafka.dws.user.visit");
        final int WIN_MIN      = ConfigUtils.getInt("dws.window.minutes");

        final WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, ts) -> {
                    try {
                        return tsOf(JSON.parseObject(s));
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                })
                .withIdleness(Duration.ofSeconds(10));

        // Sources
        KafkaSource<String> pageSrc = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_PAGE, UUID.randomUUID().toString(), OffsetsInitializer.earliest());
        KafkaSource<String> startSrc = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_START, UUID.randomUUID().toString(), OffsetsInitializer.earliest());

        DataStream<JSONObject> page = env.fromSource(pageSrc, wm, "dwd-log-page")
                .map(JSON::parseObject);
        DataStream<JSONObject> start = env.fromSource(startSrc, wm, "dwd-log-start")
                .map(JSON::parseObject);

        // 窗口（全窗口聚合，若要按渠道/设备拆分可改成 keyBy 再 window）
        AllWindowedStream<JSONObject, TimeWindow> pageWin =
                page.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(WIN_MIN)));
        AllWindowedStream<JSONObject, TimeWindow> startWin =
                start.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(WIN_MIN)));

        // 页面窗口 → PV / UV / duration_sum / bounce 近似
        DataStream<JSONObject> pageAgg = pageWin.process(new ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>() {
            @Override
            public void process(Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                long pv = 0L;
                long durationSum = 0L;
                Set<String> uvSet = new HashSet<>();
                Map<String, Integer> sidCount = new HashMap<>();
                for (JSONObject j : elements) {
                    pv++;
                    durationSum += pageDurationOf(j);
                    uvSet.add(visitorIdOf(j));
                    String sid = sidOf(j);
                    if (sid != null && !sid.isEmpty()) {
                        sidCount.put(sid, sidCount.getOrDefault(sid, 0) + 1);
                    }
                }
                long bounce = sidCount.values().stream().filter(c -> c == 1).count();

                JSONObject o = new JSONObject(true);
                o.fluentPut("stt", ctx.window().getStart())
                 .fluentPut("edt", ctx.window().getEnd())
                 .fluentPut("pv", pv)
                 .fluentPut("uv", (long) uvSet.size())
                 .fluentPut("duration_ms", durationSum)
                 .fluentPut("bounce_ct", bounce);
                out.collect(o);
            }
        });

        // 启动窗口 → 会话数（按 sid 去重）
        DataStream<JSONObject> startAgg = startWin.process(new ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>() {
            @Override
            public void process(Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                Set<String> sidSet = new HashSet<>();
                for (JSONObject j : elements) {
                    String sid = sidOf(j);
                    if (sid != null && !sid.isEmpty()) {
                        sidSet.add(sid);
                    }
                }
                JSONObject o = new JSONObject(true);
                o.fluentPut("stt", ctx.window().getStart())
                 .fluentPut("edt", ctx.window().getEnd())
                 .fluentPut("session_ct", (long) sidSet.size());
                out.collect(o);
            }
        });

        // 合并两路指标（按 stt|edt 汇总）
        DataStream<JSONObject> merged = pageAgg.union(startAgg)
                .keyBy(j -> j.getLongValue("stt") + "|" + j.getLongValue("edt"))
                .reduce((a, b) -> {
                    JSONObject r = new JSONObject(true);
                    long stt = a.getLongValue("stt"); long edt = a.getLongValue("edt");
                    r.fluentPut("stt", stt).fluentPut("edt", edt)
                     .fluentPut("pv", a.getLongValue("pv") + b.getLongValue("pv"))
                     .fluentPut("uv", a.getLongValue("uv") + b.getLongValue("uv"))
                     .fluentPut("duration_ms", a.getLongValue("duration_ms") + b.getLongValue("duration_ms"))
                     .fluentPut("bounce_ct", a.getLongValue("bounce_ct") + b.getLongValue("bounce_ct"))
                     .fluentPut("session_ct", a.getLongValue("session_ct") + b.getLongValue("session_ct"));
                    return r;
                });

        // Sink 到 DWS 主题

        merged.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject jsonObject) throws Exception {
                        return jsonObject.toJSONString();
                    }
                }
        ).sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, OUT_TOPIC)).name("sink-dws-user-visit-stats");


        // 调试
        merged.map(j -> "[DWS-USER-VISIT] " + j.toJSONString()).print();

        env.execute("DWS User Visit Stats Window");
    }
}
