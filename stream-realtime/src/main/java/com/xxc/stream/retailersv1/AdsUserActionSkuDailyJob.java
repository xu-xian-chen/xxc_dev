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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.UUID;

public class AdsUserActionSkuDailyJob {

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
        EnvironmentSettingUtils.defaultParameter(env);

        final String BOOTSTRAP = ConfigUtils.getString("kafka.bootstrap.servers");
        final String SRC_TOPIC = ConfigUtils.getString("kafka.dws.user.action");
        final String CH_URL    = ConfigUtils.getString("clickhouse.url");
        final String CH_USER   = ConfigUtils.getString("clickhouse.username");
        final String CH_PWD    = ConfigUtils.getString("clickhouse.password");
        final String DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, ts) -> {
                    try { return eventTs(JSON.parseObject(s)); } catch (Exception e) { return System.currentTimeMillis(); }
                });

        KafkaSource<String> source = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_TOPIC, "ads-user-action-"+ UUID.randomUUID(), OffsetsInitializer.earliest());

        DataStream<JSONObject> dws = env.fromSource(source, wm, "src-dws-user-action")
                .map(JSON::parseObject);

        DataStream<JSONObject> daily = dws
                .keyBy(j -> j.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.hours(1)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String sku, Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long exp=0, expU=0, clk=0, clkU=0, fav=0, favU=0, cart=0, cartU=0;
                        for (JSONObject j: elements) {
                            exp  += j.getLongValue("exposure_ct");
                            expU += j.getLongValue("exposure_user_ct");
                            clk  += j.getLongValue("click_ct");
                            clkU += j.getLongValue("click_user_ct");
                            fav  += j.getLongValue("favor_ct");
                            favU += j.getLongValue("favor_user_ct");
                            cart += j.getLongValue("cart_ct");
                            cartU+= j.getLongValue("cart_user_ct");
                        }
                        java.time.LocalDate dt = java.time.Instant.ofEpochMilli(ctx.window().getEnd())
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("dt", dt.toString())
                         .fluentPut("sku_id", sku)
                         .fluentPut("exposure_ct", exp)
                         .fluentPut("exposure_user_ct", expU)
                         .fluentPut("click_ct", clk)
                         .fluentPut("click_user_ct", clkU)
                         .fluentPut("favor_ct", fav)
                         .fluentPut("favor_user_ct", favU)
                         .fluentPut("cart_ct", cart)
                         .fluentPut("cart_user_ct", cartU);
                        out.collect(o);
                    }
                });

        daily.addSink(JdbcSink.sink(
                "INSERT INTO ads_user_action_sku_day " +
                        "(dt,sku_id,exposure_ct,exposure_user_ct,click_ct,click_user_ct,favor_ct,favor_user_ct,cart_ct,cart_user_ct) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (JdbcStatementBuilder<JSONObject>) (ps, j) -> {
                    ps.setString(1,  j.getString("dt"));
                    ps.setString(2,  j.getString("sku_id"));
                    ps.setLong(3,    j.getLongValue("exposure_ct"));
                    ps.setLong(4,    j.getLongValue("exposure_user_ct"));
                    ps.setLong(5,    j.getLongValue("click_ct"));
                    ps.setLong(6,    j.getLongValue("click_user_ct"));
                    ps.setLong(7,    j.getLongValue("favor_ct"));
                    ps.setLong(8,    j.getLongValue("favor_user_ct"));
                    ps.setLong(9,    j.getLongValue("cart_ct"));
                    ps.setLong(10,   j.getLongValue("cart_user_ct"));
                },
                JdbcExecutionOptions.builder().withBatchSize(200).withBatchIntervalMs(2000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(CH_URL).withDriverName(DRIVER)
                        .withUsername(CH_USER).withPassword(CH_PWD).build()
        )).name("sink-ads-user-action-sku-day");

        daily.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                return jsonObject.toJSONString();
            }
        }).print();

        env.execute("ADS User Action SKU Daily");
    }
}
