package com.xxc.stream;


import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadMysqlCDC2Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    }
}
