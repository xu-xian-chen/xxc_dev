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

public class AdsTradeSkuDailyJob {

    // ---- 安全取配置 ----
    private static String cfg(String k, String d){
        try { String v = com.xxc.utils.ConfigUtils.getString(k); return (v==null||v.trim().isEmpty())?d:v; }
        catch (Throwable t) { return d; }
    }

    // ---- 模型 ----
    public static class DailyAgg {
        public String dt; public String skuId;
        public long orderCt, orderUserCt, skuNum; public double orderAmount;
        public long paymentCt, paymentUserCt; public double paymentAmount;
        public long refundCt, refundUserCt; public double refundAmount;
        DailyAgg() {}
        DailyAgg(String dt, String sku){ this.dt=dt; this.skuId=sku; }
        void add(DailyAgg o){
            orderCt+=o.orderCt; orderUserCt+=o.orderUserCt; skuNum+=o.skuNum; orderAmount+=o.orderAmount;
            paymentCt+=o.paymentCt; paymentUserCt+=o.paymentUserCt; paymentAmount+=o.paymentAmount;
            refundCt+=o.refundCt; refundUserCt+=o.refundUserCt; refundAmount+=o.refundAmount;
        }
        JSONObject toJson(){
            return new JSONObject(true)
                    .fluentPut("dt", dt).fluentPut("sku_id", skuId)
                    .fluentPut("order_ct", orderCt).fluentPut("order_user_ct", orderUserCt)
                    .fluentPut("sku_num", skuNum).fluentPut("order_amount", orderAmount)
                    .fluentPut("payment_ct", paymentCt).fluentPut("payment_user_ct", paymentUserCt)
                    .fluentPut("payment_amount", paymentAmount)
                    .fluentPut("refund_ct", refundCt).fluentPut("refund_user_ct", refundUserCt)
                    .fluentPut("refund_amount", refundAmount);
        }
    }

    // ---- 工具：从 DWS 转 DailyAgg ----
    private static final DateTimeFormatter DT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static String toDt(long millis, ZoneId tz){ return Instant.ofEpochMilli(millis).atZone(tz).toLocalDate().format(DT); }

    private static DailyAgg fromDwsOrder(JSONObject j, ZoneId tz){
        String dt = toDt(j.getLongValue("edt"), tz);
        DailyAgg d = new DailyAgg(dt, j.getString("sku_id"));
        d.orderCt       = j.getLongValue("order_ct");
        d.orderUserCt   = j.getLongValue("order_user_ct");
        d.skuNum        = j.getLongValue("sku_num");
        d.orderAmount   = j.getDoubleValue("order_amount");
        return d;
    }
    private static DailyAgg fromDwsPayment(JSONObject j, ZoneId tz){
        String dt = toDt(j.getLongValue("edt"), tz);
        DailyAgg d = new DailyAgg(dt, j.getString("sku_id"));
        d.paymentCt     = j.getLongValue("payment_ct");
        d.paymentUserCt = j.getLongValue("payment_user_ct");
        d.paymentAmount = j.getDoubleValue("payment_amount");
        return d;
    }
    private static DailyAgg fromDwsRefund(JSONObject j, ZoneId tz){
        String dt = toDt(j.getLongValue("edt"), tz);
        DailyAgg d = new DailyAgg(dt, j.getString("sku_id"));
        d.refundCt      = j.getLongValue("refund_ct");
        d.refundUserCt  = j.getLongValue("refund_user_ct");
        d.refundAmount  = j.getDoubleValue("refund_amount");
        return d;
    }

    // ---- Upsert 累计 ----
    public static class DailyUpserter extends KeyedProcessFunction<String, DailyAgg, JSONObject> {
        private transient ValueState<DailyAgg> state;
        private final int ttlDays;
        DailyUpserter(int ttlDays){ this.ttlDays = ttlDays; }
        @Override public void open(Configuration parameters){
            ValueStateDescriptor<DailyAgg> desc = new ValueStateDescriptor<>("ads_sku_day", DailyAgg.class);
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(ttlDays))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
            desc.enableTimeToLive(ttl);
            state = getRuntimeContext().getState(desc);
        }
        @Override public void processElement(DailyAgg in, Context ctx, Collector<JSONObject> out) throws Exception {
            DailyAgg cur = state.value();
            if (cur == null) cur = new DailyAgg(in.dt, in.skuId);
            cur.add(in);
            state.update(cur);
            out.collect(cur.toJson());
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 配置
        final String BOOTSTRAP = cfg("kafka.bootstrap.servers","cdh01:9092,cdh02:9092,cdh03:9092");
        final String DWS_ORDER = cfg("kafka.dws.order_stats","dws_order_stats");
        final String DWS_PAY   = cfg("kafka.dws.payment_stats","dws_payment_stats");
        final String DWS_REF   = cfg("kafka.dws.refund_stats","dws_refund_stats"); // 可无
        final String CH_SQL    = cfg("clickhouse.insert.ads_trade_sku_day",
                "INSERT INTO ads_trade_sku_day (dt,sku_id,order_ct,order_user_ct,sku_num,order_amount," +
                        "payment_ct,payment_user_ct,payment_amount,refund_ct,refund_user_ct,refund_amount) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        final int TTL_DAYS     = Integer.parseInt(cfg("ads.state.ttl.days","3"));
        final java.time.ZoneId ZONE = java.time.ZoneId.of(cfg("ads.timezone","Asia/Shanghai"));

        // 统一水位：用 DWS 的窗口结束时间 edt 作为时间戳
        WatermarkStrategy<String> wm = WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner((s, ts) -> { try { return JSON.parseObject(s).getLongValue("edt"); } catch(Exception e){ return 0L; }})
                .withIdleness(Duration.ofSeconds(5));

        // 源
        KafkaSource<String> odSrc  = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, DWS_ORDER, new Date().toString(), OffsetsInitializer.earliest());
        KafkaSource<String> paySrc = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, DWS_PAY,   new Date().toString(), OffsetsInitializer.earliest());

        DataStream<DailyAgg> fromOrder = env.fromSource(odSrc, wm, "dws-order")
                .map((MapFunction<String, DailyAgg>) s -> fromDwsOrder(JSON.parseObject(s), ZONE));
        DataStream<DailyAgg> fromPay   = env.fromSource(paySrc, wm, "dws-payment")
                .map((MapFunction<String, DailyAgg>) s -> fromDwsPayment(JSON.parseObject(s), ZONE));

        DataStream<DailyAgg> merged = fromOrder.union(fromPay);

        // 可选 refund（topic 不存在时，构造 source 会抛异常，这里 try 一下）
        try {
            KafkaSource<String> refSrc = KafkaUtils.buildKafkaSecureSource(BOOTSTRAP, DWS_REF, new Date().toString(), OffsetsInitializer.earliest());
            DataStream<DailyAgg> fromRefund = env.fromSource(refSrc, wm, "dws-refund")
                    .map((MapFunction<String, DailyAgg>) s -> fromDwsRefund(JSON.parseObject(s), ZONE));
            merged = merged.union(fromRefund);
        } catch (Throwable ignore){ /* 没有退款主题就跳过 */ }

        // 按 dt|sku 累计，并直接落 ClickHouse
        SingleOutputStreamOperator<JSONObject> upserts = merged
                .keyBy(d -> d.dt + "|" + d.skuId)
                .process(new DailyUpserter(TTL_DAYS));
        upserts.print();
        upserts.addSink(
                ClickHouseUtils.buildSink(CH_SQL, (ps, j) -> {
                    ps.setString(1,  j.getString("dt"));
                    ps.setString(2,  j.getString("sku_id"));
                    ps.setLong  (3,  j.getLongValue("order_ct"));
                    ps.setLong  (4,  j.getLongValue("order_user_ct"));
                    ps.setLong  (5,  j.getLongValue("sku_num"));
                    ps.setBigDecimal(6,  j.getBigDecimal("order_amount"));
                    ps.setLong  (7,  j.getLongValue("payment_ct"));
                    ps.setLong  (8,  j.getLongValue("payment_user_ct"));
                    ps.setBigDecimal(9,  j.getBigDecimal("payment_amount"));
                    ps.setLong  (10, j.getLongValue("refund_ct"));
                    ps.setLong  (11, j.getLongValue("refund_user_ct"));
                    ps.setBigDecimal(12, j.getBigDecimal("refund_amount"));
                })
        ).name("sink-ch-ads_trade_sku_day");

        env.execute("ADS Trade SKU Daily → ClickHouse");
    }
}
