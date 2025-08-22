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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
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

public class DwsRefundStatsWindowJob {

    // ---- safe config helpers ----
    private static String cfg(String key, String def) {
        try {
            String v = com.xxc.utils.ConfigUtils.getString(key);
            return (v == null || v.trim().isEmpty()) ? def : v;
        } catch (Throwable t) {
            return def;
        }
    }
    private static boolean topicExists(String bootstrap, String topic) {
        Properties p = new Properties();
        p.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(p)) {
            return admin.listTopics().names().get().contains(topic);
        } catch (Exception e) { return false; }
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
        final String SRC_REF   = cfg("kafka.dwd.trade.refund_info",
                "dwd_trade_refund_info");    // 可能不存在
        final String SRC_OD    = cfg("kafka.dwd.trade.order_detail",
                "dwd_trade_order_detail");
        final String SINK_DWS  = cfg("kafka.dws.refund_stats",
                "dws_refund_stats");
        final int WINDOW_MIN   = Integer.parseInt(cfg("dws.window.minutes", "1"));
        final int JOIN_HOURS   = Integer.parseInt(cfg("dws.refund.join.hours", "2"));
        final boolean DEBUG    = Boolean.parseBoolean(cfg("dws.debug", "false"));
        final boolean DEBUG_PT = Boolean.parseBoolean(cfg("dws.debug_processing", "false"));

        // 源不存在则直接退出，避免 UnknownTopic 把作业拉闸
        if (!topicExists(BOOTSTRAP, SRC_REF)) {
            System.out.println("[DWS] 跳过退款统计：Kafka 不存在主题 " + SRC_REF);
            return;
        }

        ensureTopicExists(BOOTSTRAP, SINK_DWS, 3, (short)1);

        KafkaSource<String> refSource = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_REF, new Date().toString(), OffsetsInitializer.earliest());
        KafkaSource<String> odSource  = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, SRC_OD, new Date().toString(), OffsetsInitializer.earliest());

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((s, ts) -> {
                    try { return JSON.parseObject(s).getLongValue("event_time"); }
                    catch (Exception e) { return 0L; }
                })
                .withIdleness(Duration.ofSeconds(5));

        DataStream<JSONObject> refund = env.fromSource(refSource, wm, "src-dwd-refund-info")
                .map(new MapFunction<String, JSONObject>() {
                    @Override public JSONObject map(String s) { return JSON.parseObject(s); }
                });
        DataStream<JSONObject> od = env.fromSource(odSource, wm, "src-dwd-order-detail")
                .map(new MapFunction<String, JSONObject>() {
                    @Override public JSONObject map(String s) { return JSON.parseObject(s); }
                });

        if (DEBUG) {
            refund.map(j -> "[REF] " + j.toJSONString()).returns(String.class).print().name("print-ref").setParallelism(1);
            od    .map(j -> "[OD ] " + j.toJSONString()).returns(String.class).print().name("print-od").setParallelism(1);
        }

        // refund × od (order_id) → sku 级退款明细
        SingleOutputStreamOperator<JSONObject> refundDetail = refund
                .keyBy(j -> j.getString("order_id"))
                .intervalJoin(od.keyBy(j -> j.getString("order_id")))
                .between(Time.hours(-JOIN_HOURS), Time.hours(JOIN_HOURS))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject r, JSONObject d, Context ctx, Collector<JSONObject> out) {
                        JSONObject row = new JSONObject(true);
                        row.fluentPut("sku_id", d.getString("sku_id"))
                                .fluentPut("user_id", r.getString("user_id"))
                                .fluentPut("refund_amount", r.getBigDecimal("refund_amount"))
                                .fluentPut("event_time", r.getLongValue("event_time"));
                        out.collect(row);
                    }
                });

        SingleOutputStreamOperator<String> result =
                (DEBUG_PT
                        ? refundDetail.keyBy(j -> j.getString("sku_id"))
                        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_MIN)))
                        .aggregate(new RefundAgg(), new RefundWinOut())
                        : refundDetail.keyBy(j -> j.getString("sku_id"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MIN)))
                        .aggregate(new RefundAgg(), new RefundWinOut())
                );

        result.sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, SINK_DWS))
                .name("sink-dws-refund-stats");

        env.execute("DWS Refund Stats Window");
    }

    // ---- agg & out ----
    static class RefundAcc { long ct; double amount; Set<String> users = new HashSet<String>(); }
    static class RefundAgg implements AggregateFunction<JSONObject, RefundAcc, RefundAcc> {
        @Override public RefundAcc createAccumulator() { return new RefundAcc(); }
        @Override public RefundAcc add(JSONObject j, RefundAcc a) {
            a.ct += 1; a.amount += j.getDoubleValue("refund_amount");
            String uid = j.getString("user_id"); if (uid != null) a.users.add(uid);
            return a;
        }
        @Override public RefundAcc getResult(RefundAcc a) { return a; }
        @Override public RefundAcc merge(RefundAcc a, RefundAcc b) {
            a.ct += b.ct; a.amount += b.amount; a.users.addAll(b.users); return a;
        }
    }
    static class RefundWinOut extends ProcessWindowFunction<RefundAcc, String, String, TimeWindow> {
        @Override public void process(String skuId, Context ctx, Iterable<RefundAcc> it, Collector<String> out) {
            RefundAcc a = it.iterator().next();
            JSONObject row = new JSONObject(true);
            row.fluentPut("stt", ctx.window().getStart())
                    .fluentPut("edt", ctx.window().getEnd())
                    .fluentPut("sku_id", skuId)
                    .fluentPut("refund_ct", a.ct)
                    .fluentPut("refund_user_ct", a.users.size())
                    .fluentPut("refund_amount", a.amount);
            out.collect(row.toJSONString());
        }
    }
}
