package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.EnvironmentSettingUtils;
import com.xxc.utils.KafkaUtils;
import com.xxc.utils.ClickHouseUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class AdsTradeOverviewDailyJob {

    private static String cfg(String k, String d){
        try { String v = com.xxc.utils.ConfigUtils.getString(k); return (v==null||v.trim().isEmpty())?d:v; }
        catch (Throwable t) { return d; }
    }

    // —— 把 sku 日明细累加成日总览（直接从 DWS 汇总，避免依赖中间 Kafka） ——
    public static class Totals {
        public String dt;
        public long orderCt, orderUserCt, skuNum; public double orderAmount;
        public long paymentCt, paymentUserCt; public double paymentAmount;
        public long refundCt, refundUserCt; public double refundAmount;
        void addFromOrder(JSONObject j){ orderCt+=j.getLongValue("order_ct"); orderUserCt+=j.getLongValue("order_user_ct"); skuNum+=j.getLongValue("sku_num"); orderAmount+=j.getDoubleValue("order_amount"); }
        void addFromPayment(JSONObject j){ paymentCt+=j.getLongValue("payment_ct"); paymentUserCt+=j.getLongValue("payment_user_ct"); paymentAmount+=j.getDoubleValue("payment_amount"); }
        void addFromRefund(JSONObject j){ refundCt+=j.getLongValue("refund_ct"); refundUserCt+=j.getLongValue("refund_user_ct"); refundAmount+=j.getDoubleValue("refund_amount"); }
        JSONObject toJson(){
            JSONObject o=new JSONObject(true);
            o.fluentPut("dt", dt)
             .fluentPut("order_ct", orderCt).fluentPut("order_user_ct", orderUserCt).fluentPut("sku_num", skuNum).fluentPut("order_amount", orderAmount)
             .fluentPut("payment_ct", paymentCt).fluentPut("payment_user_ct", paymentUserCt).fluentPut("payment_amount", paymentAmount)
             .fluentPut("refund_ct", refundCt).fluentPut("refund_user_ct", refundUserCt).fluentPut("refund_amount", refundAmount)
             .fluentPut("arp", paymentUserCt==0?0:paymentAmount/Math.max(paymentUserCt,1)); // 例：人均支付额
            return o;
        }
    }

    public static class DailyTotalUpserter extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient ValueState<Totals> state;
        private final int ttlDays;
        DailyTotalUpserter(int ttlDays){ this.ttlDays = ttlDays; }
        @Override public void open(Configuration parameters){
            ValueStateDescriptor<Totals> desc=new ValueStateDescriptor<>("ads_overview_day", Totals.class);
            StateTtlConfig ttl=StateTtlConfig.newBuilder(Time.days(ttlDays))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
            desc.enableTimeToLive(ttl);
            state=getRuntimeContext().getState(desc);
        }
        @Override public void processElement(JSONObject in, Context ctx, Collector<JSONObject> out) throws Exception {
            Totals t = state.value();
            if (t==null){ t=new Totals(); t.dt=in.getString("dt"); }
            // 哪类来源
            String src = in.getString("__src__");
            if ("ORDER".equals(src)) t.addFromOrder(in);
            else if ("PAY".equals(src)) t.addFromPayment(in);
            else if ("REF".equals(src)) t.addFromRefund(in);
            state.update(t);
            out.collect(t.toJson());
        }
    }

    // 工具：把 edt 转日期
    private static final DateTimeFormatter DT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static String toDt(long millis, ZoneId tz){ return Instant.ofEpochMilli(millis).atZone(tz).toLocalDate().format(DT); }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        final String BOOTSTRAP = cfg("kafka.bootstrap.servers","cdh01:9092,cdh02:9092,cdh03:9092");
        final String DWS_ORDER = cfg("kafka.dws.order_stats","dws_order_stats");
        final String DWS_PAY   = cfg("kafka.dws.payment_stats","dws_payment_stats");
        final String DWS_REF   = cfg("kafka.dws.refund_stats","dws_refund_stats"); // 可无
        final String CH_SQL    = cfg("clickhouse.insert.ads_trade_overview_day",
                "INSERT INTO ads_trade_overview_day (dt,order_ct,order_user_ct,sku_num,order_amount," +
                        "payment_ct,payment_user_ct,payment_amount,refund_ct,refund_user_ct,refund_amount,arp) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        final int TTL_DAYS     = Integer.parseInt(cfg("ads.state.ttl.days","3"));
        final java.time.ZoneId ZONE = java.time.ZoneId.of(cfg("ads.timezone","Asia/Shanghai"));

        WatermarkStrategy<String> wm = WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner((s, ts) -> { try { return JSON.parseObject(s).getLongValue("edt"); } catch(Exception e){ return 0L; }})
                .withIdleness(Duration.ofSeconds(5));

        // 构造三条标准化流：都转成 {"dt":..., 指标..., "__src__": "ORDER|PAY|REF"}
        KafkaSource<String> odSrc  = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, DWS_ORDER, new Date().toString(), OffsetsInitializer.earliest());
        KafkaSource<String> paySrc = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, DWS_PAY,   new Date().toString(), OffsetsInitializer.earliest());

        DataStream<JSONObject> od = env.fromSource(odSrc, wm, "dws-order")
                .map((MapFunction<String, JSONObject>) s -> {
                    JSONObject j = JSON.parseObject(s);
                    JSONObject o = new JSONObject(true);
                    o.fluentPut("dt", toDt(j.getLongValue("edt"), ZONE))
                     .fluentPut("order_ct", j.getLongValue("order_ct"))
                     .fluentPut("order_user_ct", j.getLongValue("order_user_ct"))
                     .fluentPut("sku_num", j.getLongValue("sku_num"))
                     .fluentPut("order_amount", j.getDoubleValue("order_amount"))
                     .fluentPut("__src__", "ORDER");
                    return o;
                });

        DataStream<JSONObject> pay = env.fromSource(paySrc, wm, "dws-payment")
                .map((MapFunction<String, JSONObject>) s -> {
                    JSONObject j = JSON.parseObject(s);
                    JSONObject o = new JSONObject(true);
                    o.fluentPut("dt", toDt(j.getLongValue("edt"), ZONE))
                     .fluentPut("payment_ct", j.getLongValue("payment_ct"))
                     .fluentPut("payment_user_ct", j.getLongValue("payment_user_ct"))
                     .fluentPut("payment_amount", j.getDoubleValue("payment_amount"))
                     .fluentPut("__src__", "PAY");
                    return o;
                });

        DataStream<JSONObject> merged = od.union(pay);

        // 可选 refund
        try {
            KafkaSource<String> refSrc = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, DWS_REF, new Date().toString(), OffsetsInitializer.earliest());
            DataStream<JSONObject> ref = env.fromSource(refSrc, wm, "dws-refund")
                    .map((MapFunction<String, JSONObject>) s -> {
                        JSONObject j = JSON.parseObject(s);
                        JSONObject o = new JSONObject(true);
                        o.fluentPut("dt", toDt(j.getLongValue("edt"), ZONE))
                         .fluentPut("refund_ct", j.getLongValue("refund_ct"))
                         .fluentPut("refund_user_ct", j.getLongValue("refund_user_ct"))
                         .fluentPut("refund_amount", j.getDoubleValue("refund_amount"))
                         .fluentPut("__src__", "REF");
                        return o;
                    });
            merged = merged.union(ref);
        } catch (Throwable ignore) { /* 没退款主题就跳过 */ }

        // 按 dt 累计并落盘
        SingleOutputStreamOperator<JSONObject> totals = merged
                .keyBy(j -> j.getString("dt"))
                .process(new DailyTotalUpserter(TTL_DAYS));
        totals.print();
        totals.addSink(
                ClickHouseUtils.buildSink(CH_SQL, (ps, j) -> {
                    ps.setString(1,  j.getString("dt"));
                    ps.setLong  (2,  j.getLongValue("order_ct"));
                    ps.setLong  (3,  j.getLongValue("order_user_ct"));
                    ps.setLong  (4,  j.getLongValue("sku_num"));
                    ps.setBigDecimal(5,  j.getBigDecimal("order_amount"));
                    ps.setLong  (6,  j.getLongValue("payment_ct"));
                    ps.setLong  (7,  j.getLongValue("payment_user_ct"));
                    ps.setBigDecimal(8,  j.getBigDecimal("payment_amount"));
                    ps.setLong  (9,  j.getLongValue("refund_ct"));
                    ps.setLong  (10, j.getLongValue("refund_user_ct"));
                    ps.setBigDecimal(11, j.getBigDecimal("refund_amount"));
                    ps.setBigDecimal(12, j.getBigDecimal("arp"));
                })
        ).name("sink-ch-ads_trade_overview_day");

        env.execute("ADS Trade Overview Daily → ClickHouse");
    }
}
