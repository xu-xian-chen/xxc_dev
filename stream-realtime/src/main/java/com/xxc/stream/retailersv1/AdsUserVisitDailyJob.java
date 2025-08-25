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
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.UUID;

public class AdsUserVisitDailyJob {

    private static long eventTs(JSONObject j){
        long ts = j.getLongValue("edt");
        if (ts == 0L) {
            ts = j.getLongValue("stt");
        }
        if (ts == 0L) {
            ts = j.getLongValue("ts");
        }
        return ts == 0L ? System.currentTimeMillis() : ts;
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        EnvironmentSettingUtils.defaultParameter(env);

        final String BOOTSTRAP = ConfigUtils.getString("kafka.bootstrap.servers");
        final String SRC_TOPIC = ConfigUtils.getString("kafka.dws.user.visit");
        final String CH_URL    = ConfigUtils.getString("clickhouse.url");
        final String CH_USER   = ConfigUtils.getString("clickhouse.username");
        final String CH_PWD    = ConfigUtils.getString("clickhouse.password");
        final String DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
        System.out.println("[CFG] bootstrap=" + BOOTSTRAP);
        System.out.println("[CFG] topic=" + SRC_TOPIC);

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, ts) -> {
                    try {
                        return eventTs(JSON.parseObject(s));
                    }
                    catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });

        KafkaSource<String> source = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_TOPIC, "ads-user-visit-"+ UUID.randomUUID(), OffsetsInitializer.earliest());

        DataStream<JSONObject> dws = env.fromSource(source, wm, "src-dws-user-visit")
                .map(JSON::parseObject);
        dws.print("dws---->");
        DataStream<JSONObject> daily = dws
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.hours(1)))
                .process(new ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>() {
                    @Override
                    public void process(Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long pv=0, uv=0, session=0, dur=0, bounce=0;
                        for (JSONObject j: elements) {
                            pv += j.getLongValue("pv");
                            uv += j.getLongValue("uv");
                            session += j.getLongValue("session_ct");
                            dur += j.getLongValue("duration_ms");
                            bounce += j.getLongValue("bounce_ct");
                        }
                        java.time.LocalDate dt = java.time.Instant.ofEpochMilli(ctx.window().getEnd())
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("dt", dt.toString())
                         .fluentPut("pv", pv)
                         .fluentPut("uv", uv)
                         .fluentPut("session_ct", session)
                         .fluentPut("duration_ms", dur)
                         .fluentPut("bounce_ct", bounce);
                        out.collect(o);
                    }
                });

        daily.addSink(JdbcSink.sink(
                "INSERT INTO ads_user_visit_day (dt,pv,uv,session_ct,duration_ms,bounce_ct) VALUES (?,?,?,?,?,?)",
                (JdbcStatementBuilder<JSONObject>) (ps, j) -> {
                    ps.setString(1, j.getString("dt"));
                    ps.setLong(2, j.getLongValue("pv"));
                    ps.setLong(3, j.getLongValue("uv"));
                    ps.setLong(4, j.getLongValue("session_ct"));
                    ps.setLong(5, j.getLongValue("duration_ms"));
                    ps.setLong(6, j.getLongValue("bounce_ct"));
                },
                JdbcExecutionOptions.builder().withBatchSize(200).withBatchIntervalMs(2000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(CH_URL).withDriverName(DRIVER)
                        .withUsername(CH_USER).withPassword(CH_PWD).build()
        )).name("sink-ads-user-visit-day");

        daily.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                return jsonObject.toJSONString();
            }
        }).print();

        env.execute("ADS User Visit Daily");
    }
}
