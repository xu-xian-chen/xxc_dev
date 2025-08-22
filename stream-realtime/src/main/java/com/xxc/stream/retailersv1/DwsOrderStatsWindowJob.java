package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.EnvironmentSettingUtils;
import com.xxc.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class DwsOrderStatsWindowJob {

    // ---- safe config helpers ----
    private static String cfg(String key, String def) {
        try {
            String v = com.xxc.utils.ConfigUtils.getString(key);
            return (v == null || v.trim().isEmpty()) ? def : v;
        } catch (Throwable t) {
            return def;
        }
    }
    private static void ensureTopicExists(String bootstrap, String topic, int partitions, short repl) {
        Properties p = new Properties();
        p.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(p)) {
            java.util.Set<String> names = admin.listTopics().names().get();
            if (!names.contains(topic)) {
                org.apache.kafka.clients.admin.NewTopic nt = new org.apache.kafka.clients.admin.NewTopic(topic, partitions, repl);
                try { admin.createTopics(java.util.Collections.singleton(nt)).all().get(); }
                catch (ExecutionException ignore) {}
            }
        } catch (Exception ignore) {}
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        final String BOOTSTRAP = cfg("kafka.bootstrap.servers",
                "cdh01:9092,cdh02:9092,cdh03:9092");
        final String SRC_OD    = cfg("kafka.dwd.trade.order_detail",
                "dwd_trade_order_detail");
        final String SINK_DWS  = cfg("kafka.dws.order_stats",
                "dws_order_stats");
        final int WINDOW_MIN   = Integer.parseInt(cfg("dws.window.minutes", "1"));
        final boolean DEBUG    = Boolean.parseBoolean(cfg("dws.debug", "false"));
        final boolean DEBUG_PT = Boolean.parseBoolean(cfg("dws.debug_processing", "false"));

        ensureTopicExists(BOOTSTRAP, SINK_DWS, 3, (short)1);

        KafkaSource<String> odSource = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_OD, new Date().toString(), OffsetsInitializer.earliest());

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((s, ts) -> {
                    try { return JSON.parseObject(s).getLongValue("event_time"); }
                    catch (Exception e) { return 0L; }
                })
                .withIdleness(Duration.ofSeconds(5)); // 关键：低流量也能触发窗口

        DataStream<JSONObject> od = env.fromSource(odSource, wm, "src-dwd-order-detail")
                .map(new MapFunction<String, JSONObject>() {
                    @Override public JSONObject map(String s) { return JSON.parseObject(s); }
                });

        if (DEBUG) {
            od.map(j -> "[OD] " + j.toJSONString()).returns(String.class)
                    .print().name("print-od").setParallelism(1);
        }

        SingleOutputStreamOperator<String> result =
                (DEBUG_PT
                        ? od.keyBy(j -> j.getString("sku_id"))
                        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_MIN)))
                        .aggregate(new OrderAgg(), new OrderWinOut())
                        : od.keyBy(j -> j.getString("sku_id"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MIN)))
                        .aggregate(new OrderAgg(), new OrderWinOut())
                );

        result.sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, SINK_DWS))
                .name("sink-dws-order-stats");

        env.execute("DWS Order Stats Window");
    }

    // ---- agg & out ----
    static class OrderAcc { long orderCt; long skuNum; double amount; Set<String> users = new HashSet<String>(); }
    static class OrderAgg implements AggregateFunction<JSONObject, OrderAcc, OrderAcc> {
        @Override public OrderAcc createAccumulator() { return new OrderAcc(); }
        @Override public OrderAcc add(JSONObject j, OrderAcc a) {
            a.orderCt += 1;
            a.skuNum  += j.getLongValue("sku_num");
            if (j.containsKey("split_total_amount"))
                a.amount += j.getDoubleValue("split_total_amount");
            else
                a.amount += j.getDoubleValue("order_price") * j.getLongValue("sku_num");
            String uid = j.getString("user_id");
            if (uid != null) a.users.add(uid);
            return a;
        }
        @Override public OrderAcc getResult(OrderAcc a) { return a; }
        @Override public OrderAcc merge(OrderAcc a, OrderAcc b) {
            a.orderCt += b.orderCt; a.skuNum += b.skuNum; a.amount += b.amount; a.users.addAll(b.users); return a;
        }
    }
    static class OrderWinOut extends ProcessWindowFunction<OrderAcc, String, String, TimeWindow> {
        @Override public void process(String skuId, Context ctx, Iterable<OrderAcc> it, Collector<String> out) {
            OrderAcc a = it.iterator().next();
            JSONObject row = new JSONObject(true);
            row.fluentPut("stt", ctx.window().getStart())
                    .fluentPut("edt", ctx.window().getEnd())
                    .fluentPut("sku_id", skuId)
                    .fluentPut("order_ct", a.orderCt)
                    .fluentPut("order_user_ct", a.users.size())
                    .fluentPut("sku_num", a.skuNum)
                    .fluentPut("order_amount", a.amount);
            out.collect(row.toJSONString());
        }
    }
}
