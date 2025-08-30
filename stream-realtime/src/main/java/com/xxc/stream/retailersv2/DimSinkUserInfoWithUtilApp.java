package com.xxc.stream.retailersv2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.ConfigUtils;
import com.xxc.utils.HbaseUtils;
import com.xxc.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.joda.time.DateTime;


import java.util.Map;
/**
 * Title: DimSinkUserInfoWithUtilApp
 * Author  xuxianchen
 * Package com.xxc.stream.retailersv2
 * Date  2025/8/30 8:58
 * description:
 */
public class DimSinkUserInfoWithUtilApp {

    // ===== 配置区（按需改） =====
    private static final int SALT_BUCKETS = 16;
    private static final String TOPIC = "ods_mysql_ods_user_info";

    private static final OutputTag<String> DIRTY = new OutputTag<String>("dirty"){};

    public static void main(String[] args) throws Exception {
        // 0) 环境 & Checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(60_000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 如需：env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink-ckpt/dim_user_info");

        // 1) Source：Kafka（ODS after）
        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        TOPIC,
                        DateTime.now().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_source_user_info");

        // 2) 解析 & 过滤（仅 c/u），产出 (rowKey, colsJson)
        SingleOutputStreamOperator<Pair> parsed = source.process(
                new org.apache.flink.streaming.api.functions.ProcessFunction<String, Pair>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Pair> out) {
                        try {
                            JSONObject obj = JSON.parseObject(value);
                            String op = obj.getString("__op");
                            if (!"c".equalsIgnoreCase(op) && !"u".equalsIgnoreCase(op)) {
                                return; // delete 不处理；如需 delete，可在此侧输出
                            }
                            // 取主键：优先 user_id，再退 __pk
                            String userId = obj.getString("user_id");
                            if (StringUtils.isBlank(userId)) {
                                userId = obj.getString("__pk");
                            }
                            if (StringUtils.isBlank(userId)) {
                                ctx.output(DIRTY, "missing_pk|" + value);
                                return;
                            }
                            // 生成加盐 rowkey：dim_user_info_<id>_<bucket>
                            String rowKey = buildSaltRowKey("dim_user_info", userId);

                            // 组装要写入的列：剔除 __ 技术字段
                            JSONObject cols = new JSONObject();
                            for (Map.Entry<String, Object> e : obj.entrySet()) {
                                String k = e.getKey();
                                if (k.startsWith("__")) {
                                    continue;
                                }
                                cols.put(k, e.getValue());
                            }
                            out.collect(new Pair(rowKey, cols));
                        } catch (Exception ex) {
                            ctx.output(DIRTY, "json_parse_fail|" + value);
                        }
                    }
                }).name("parse_to_rowkey_and_cols");

        parsed.getSideOutput(DIRTY).print("dirty=>");
        // 1) 增强：可读打印
        parsed.map(p -> "[HBASE-UPsert] rk=" + p.rowKey + " cols=" + p.cols.toJSONString())
                .returns(String.class)
                .name("debug_print_cols")
                .print();

// 2) HBase Sink（替换你现有的 RichSinkFunction 实现）
        parsed.addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<Pair>() {
            private transient com.xxc.utils.HbaseUtils hb;
            private transient org.apache.hadoop.hbase.client.BufferedMutator mutator;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                // —— 显式设置 HBase 关键参数（本地/IDEA 运行时非常重要）——
                org.apache.hadoop.conf.Configuration hConf = org.apache.hadoop.hbase.HBaseConfiguration.create();
                // 你的 zookeeper 列表（host1,host2,host3），也可从 ConfigUtils 拿
                hConf.set("hbase.zookeeper.quorum", ConfigUtils.getString("zookeeper.server.host.list"));
                // 如有端口/父节点：
                // hConf.set("hbase.zookeeper.property.clientPort", "2181");
                // hConf.set("zookeeper.znode.parent", "/hbase");

                // 用你自己的工具类初始化（确保它能接受 hConf，若无此构造，至少内部要用到上面的 quorum）
                hb = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));

                // 命名空间/表/列族一致
                hb.createNamespaceIfAbsent("retailersv2");
                hb.createTable("retailersv2", "dim_user_info", "info");

                // —— 真·4MB 缓冲区（4 * 1024 * 1024）——
                org.apache.hadoop.hbase.client.BufferedMutatorParams params =
                        new org.apache.hadoop.hbase.client.BufferedMutatorParams(
                                org.apache.hadoop.hbase.TableName.valueOf("retailersv2", "dim_user_info"))
                                .writeBufferSize(4 * 1024 * 1024L); // 4MB
                mutator = hb.getConnection().getBufferedMutator(params);
            }

            @Override
            public void invoke(Pair value, Context context) throws Exception {
                // —— 直接用原生 Put，避免工具类内部列族名不一致的坑 ——
                org.apache.hadoop.hbase.client.Put put =
                        new org.apache.hadoop.hbase.client.Put(value.rowKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                // 列族：info（与建表保持一致）
                byte[] cf = "info".getBytes(java.nio.charset.StandardCharsets.UTF_8);

                for (Map.Entry<String, Object> e : value.cols.entrySet()) {
                    String col = e.getKey();
                    Object v = e.getValue();
                    if (v == null) {
                        continue;
                    }
                    put.addColumn(cf,
                            col.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                            v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                mutator.mutate(put);

                // —— 为了验证，先每条都 flush；确认通路后可改为每100条/每秒 flush ——
                mutator.flush();
            }

            @Override
            public void close() throws Exception {
                if (mutator != null) {
                    mutator.flush();
                    mutator.close();
                }
                // hb.getConnection() 若是单例/共享，可不在此关闭
            }
        }).name("hbase_dim_user_info_sink_force_flush");


        env.execute("DimSinkUserInfoWithUtilApp");
    }

    // 简单二元组
    static class Pair {
        final String rowKey;
        final JSONObject cols;
        Pair(String k, JSONObject v){ this.rowKey=k; this.cols=v; }
    }

    // 加盐 rowkey：prefix_id_bucket
    private static String buildSaltRowKey(String prefix, String id) {
        int bucket = (id.hashCode() & 0x7fffffff) % SALT_BUCKETS;
        return prefix + "_" + id + "_" + String.format("%02d", bucket);
    }
}