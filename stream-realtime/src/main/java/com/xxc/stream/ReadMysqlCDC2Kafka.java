package com.xxc.stream;

import com.xxc.utils.ConfigUtils;
import com.xxc.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadMysqlCDC2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSecureSource(
                ConfigUtils.getString("kafka.bootstrap.servers"),
                "realtime_log",
                "group1",
                OffsetsInitializer.earliest()
        );

        DataStream<String> stream = env.fromSource(
                kafkaSource,
                // 如果有事件时间，换成实际的水位线策略
                WatermarkStrategy.noWatermarks(),
                "kafka_source"
        );
        stream.print();
        env.execute();
    }
}
