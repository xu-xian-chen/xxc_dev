package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.ConfigUtils;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class DwsPaymentStatsJob {

    private static String nvl(String v, String def) { return (v == null || v.trim().isEmpty()) ? def : v; }
    private static WatermarkStrategy<String> wm() {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((s, ts) -> {
                    try { return JSON.parseObject(s).getLongValue("event_time"); }
                    catch (Exception e) { return 0L; }
                })
                .withIdleness(Duration.ofSeconds(5));
    }
    private static void ensureTopicExists(String bootstrap, String topic, int partitions, short rf) {
        Properties p = new Properties();
        p.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(p)) {
            java.util.Set<String> names = admin.listTopics().names().get();
            if (!names.contains(topic)) {
                org.apache.kafka.clients.admin.NewTopic nt = new org.apache.kafka.clients.admin.NewTopic(topic, partitions, rf);
                try { admin.createTopics(java.util.Collections.singleton(nt)).all().get();
                    System.out.println("[DWS-PAY] 已创建 Kafka 主题：" + topic);
                } catch (ExecutionException ignore) {}
            }
        } catch (Exception e) {
            System.out.println("[DWS-PAY] 创建主题失败 " + topic + " : " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        final String BOOTSTRAP   = ConfigUtils.getString("kafka.bootstrap.servers");
        final String T_DWD_OD    = ConfigUtils.getString("kafka.dwd.trade.order_detail");
        final String T_DWD_PAY   = ConfigUtils.getString("kafka.dwd.trade.payment_info");
        final String T_DWS_PAY   = ConfigUtils.getString("kafka.dws.payment_stats");
        final int WINDOW_MINUTES = Integer.parseInt(ConfigUtils.getString("dws.window.minutes"));
        ensureTopicExists(BOOTSTRAP, T_DWS_PAY, 3, (short)1);
        // 源：订单明细
        KafkaSource<String> odSource = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, T_DWD_OD, new Date().toString(), OffsetsInitializer.earliest());
        DataStream<JSONObject> od = env.fromSource(odSource, wm(), "src-dwd-order-detail")
                .map(new MapFunction<String, JSONObject>() { @Override public JSONObject map(String s) { return JSON.parseObject(s); }});

        // 源：支付
        KafkaSource<String> paySource = KafkaUtils.buildKafkaSecureSource(
                BOOTSTRAP, T_DWD_PAY, new Date().toString(), OffsetsInitializer.earliest());
        DataStream<JSONObject> pay = env.fromSource(paySource, wm(), "src-dwd-payment-info")
                .map(new MapFunction<String, JSONObject>() { @Override public JSONObject map(String s) { return JSON.parseObject(s); }});

        // join（按 order_id，把支付事件还原到 sku 级别）
        SingleOutputStreamOperator<JSONObject> payDetail = pay
                .keyBy(j -> j.getString("order_id"))
                .intervalJoin(od.keyBy(j -> j.getString("order_id")))
                .between(Time.hours(-2), Time.hours(2)) // 若时间差较大可放宽；跑通后按实际口径收紧
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject p, JSONObject d, Context ctx, Collector<JSONObject> out) {
                        JSONObject row = new JSONObject(true);
                        row.fluentPut("sku_id", d.getString("sku_id"))
                                .fluentPut("user_id", p.getString("user_id"))
                                // 若有支付明细金额字段请换掉 total_amount；没有的话这里是订单总额（会在多 sku 上重复）
                                .fluentPut("payment_amount", p.getBigDecimal("total_amount"))
                                .fluentPut("event_time", p.getLongValue("event_time"));
                        out.collect(row);
                    }
                });

        SingleOutputStreamOperator<String> dwsPayment = payDetail
                .keyBy(j -> j.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
                .aggregate(new PayAgg(), new PayWindowOut());

        dwsPayment.sinkTo(KafkaUtils.buildKafkaSink(BOOTSTRAP, T_DWS_PAY))
                .name("sink-dws-payment-stats");

        env.execute("DWS Payment Stats Job");
    }

    // ---- 聚合器 & 输出 ----
    static class PayAcc { long payCt; double payAmount; Set<String> userSet = new HashSet<String>(); }
    static class PayAgg implements AggregateFunction<JSONObject, PayAcc, PayAcc> {
        @Override public PayAcc createAccumulator() { return new PayAcc(); }
        @Override public PayAcc add(JSONObject j, PayAcc a) {
            a.payCt += 1;
            a.payAmount += j.getDoubleValue("payment_amount");
            String uid = j.getString("user_id");
            if (uid != null) {
                a.userSet.add(uid);
            }
            return a;
        }
        @Override public PayAcc getResult(PayAcc a) { return a; }
        @Override public PayAcc merge(PayAcc a, PayAcc b) {
            a.payCt += b.payCt;
            a.payAmount += b.payAmount;
            a.userSet.addAll(b.userSet);
            return a;
        }
    }
    static class PayWindowOut extends ProcessWindowFunction<PayAcc, String, String, TimeWindow> {
        @Override public void process(String skuId, Context ctx, Iterable<PayAcc> it, Collector<String> out) {
            PayAcc a = it.iterator().next();
            JSONObject row = new JSONObject(true);
            row.fluentPut("stt", ctx.window().getStart())
               .fluentPut("edt", ctx.window().getEnd())
               .fluentPut("sku_id", skuId)
               .fluentPut("payment_ct", a.payCt)
               .fluentPut("payment_user_ct", a.userSet.size())
               .fluentPut("payment_amount", a.payAmount);
            out.collect(row.toJSONString());
        }
    }
}
