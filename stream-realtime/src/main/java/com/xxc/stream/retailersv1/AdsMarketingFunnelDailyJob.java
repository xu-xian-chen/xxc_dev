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

public class AdsMarketingFunnelDailyJob {

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
        final String DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
        final String BOOTSTRAP = ConfigUtils.getString("kafka.bootstrap.servers");
        final String SRC_ACT   = ConfigUtils.getString("kafka.dws.user.action");
        final String SRC_ORD   = ConfigUtils.getString("kafka.dws.order_stats");
        final String SRC_PAY   = ConfigUtils.getString("kafka.dws.payment_stats");
        final String CH_URL    = ConfigUtils.getString("clickhouse.url");
        final String CH_USER   = ConfigUtils.getString("clickhouse.username");
        final String CH_PWD    = ConfigUtils.getString("clickhouse.password");

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, ts) -> {
                    try { return eventTs(JSON.parseObject(s)); } catch (Exception e) { return System.currentTimeMillis(); }
                });

        String gid = "ads-funnel-" + UUID.randomUUID();
        KafkaSource<String> srcAct = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, SRC_ACT, gid, OffsetsInitializer.earliest());
        KafkaSource<String> srcOrd = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, SRC_ORD, gid, OffsetsInitializer.earliest());
        KafkaSource<String> srcPay = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, SRC_PAY, gid, OffsetsInitializer.earliest());

        DataStream<JSONObject> actDws = env.fromSource(srcAct, wm, "src-dws-action").map(JSON::parseObject);
        DataStream<JSONObject> ordDws = env.fromSource(srcOrd, wm, "src-dws-order").map(JSON::parseObject);
        DataStream<JSONObject> payDws = env.fromSource(srcPay, wm, "src-dws-payment").map(JSON::parseObject);

        DataStream<JSONObject> actDaily = actDws
                .keyBy(j -> j.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.hours(1)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String sku, Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long exp=0, clk=0, cart=0, fav=0;
                        for (JSONObject j : elements) {
                            exp += j.getLongValue("exposure_ct");
                            clk += j.getLongValue("click_ct");
                            cart += j.getLongValue("cart_ct");
                            fav += j.getLongValue("favor_ct");
                        }
                        java.time.LocalDate dt = java.time.Instant.ofEpochMilli(ctx.window().getEnd())
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("dt", dt.toString()).fluentPut("sku_id", sku)
                         .fluentPut("exposure_ct", exp).fluentPut("click_ct", clk)
                         .fluentPut("cart_ct", cart).fluentPut("favor_ct", fav);
                        out.collect(o);
                    }
                });

        DataStream<JSONObject> ordDaily = ordDws
                .keyBy(j -> j.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.hours(1)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String sku, Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long orderUsers=0;
                        for (JSONObject j : elements) {
                            orderUsers += j.getLongValue("order_user_ct");
                        }
                        java.time.LocalDate dt = java.time.Instant.ofEpochMilli(ctx.window().getEnd())
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("dt", dt.toString()).fluentPut("sku_id", sku)
                         .fluentPut("order_user_ct", orderUsers);
                        out.collect(o);
                    }
                });

        DataStream<JSONObject> payDaily = payDws
                .keyBy(j -> j.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.hours(1)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String sku, Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) {
                        long payUsers=0;
                        for (JSONObject j : elements) {
                            payUsers += j.getLongValue("payment_user_ct");
                        }
                        java.time.LocalDate dt = java.time.Instant.ofEpochMilli(ctx.window().getEnd())
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("dt", dt.toString()).fluentPut("sku_id", sku)
                         .fluentPut("payment_user_ct", payUsers);
                        out.collect(o);
                    }
                });

        DataStream<JSONObject> merged = actDaily
                .union(ordDaily, payDaily)
                .keyBy(j -> j.getString("dt") + "|" + j.getString("sku_id"))
                .reduce((a, b) -> {
                    JSONObject r = new JSONObject(true);
                    String dt  = a.getString("dt")     != null ? a.getString("dt")     : b.getString("dt");
                    String sku = a.getString("sku_id") != null ? a.getString("sku_id") : b.getString("sku_id");
                    r.fluentPut("dt", dt).fluentPut("sku_id", sku)
                     .fluentPut("exposure_ct",     a.getLongValue("exposure_ct")     + b.getLongValue("exposure_ct"))
                     .fluentPut("click_ct",        a.getLongValue("click_ct")        + b.getLongValue("click_ct"))
                     .fluentPut("cart_ct",         a.getLongValue("cart_ct")         + b.getLongValue("cart_ct"))
                     .fluentPut("favor_ct",        a.getLongValue("favor_ct")        + b.getLongValue("favor_ct"))
                     .fluentPut("order_user_ct",   a.getLongValue("order_user_ct")   + b.getLongValue("order_user_ct"))
                     .fluentPut("payment_user_ct", a.getLongValue("payment_user_ct") + b.getLongValue("payment_user_ct"));
                    return r;
                });

        merged.addSink(JdbcSink.sink(
                "INSERT INTO ads_marketing_funnel_day " +
                        "(dt,sku_id,exposure_ct,click_ct,cart_ct,favor_ct,order_user_ct,payment_user_ct) VALUES (?,?,?,?,?,?,?,?)",
                (JdbcStatementBuilder<JSONObject>) (ps, j) -> {
                    ps.setString(1, j.getString("dt"));
                    ps.setString(2, j.getString("sku_id"));
                    ps.setLong(3, j.getLongValue("exposure_ct"));
                    ps.setLong(4, j.getLongValue("click_ct"));
                    ps.setLong(5, j.getLongValue("cart_ct"));
                    ps.setLong(6, j.getLongValue("favor_ct"));
                    ps.setLong(7, j.getLongValue("order_user_ct"));
                    ps.setLong(8, j.getLongValue("payment_user_ct"));
                },
                JdbcExecutionOptions.builder().withBatchSize(200).withBatchIntervalMs(2000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(CH_URL).withDriverName(DRIVER)
                        .withUsername(CH_USER).withPassword(CH_PWD).build()
        )).name("sink-ads-marketing-funnel-day");

        merged.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                return jsonObject.toJSONString();
            }
        }).print();

        env.execute("ADS Marketing Funnel Daily");
    }
}
