package com.xxc.stream.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.xxc.stream.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.xxc.stream.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.xxc.stream.retailersv1.utils.CdcSourceUtils;
import com.xxc.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.xxc.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;


/**
 * Title: DbusCdc2DimHbaseAnd2DbKafka
 * Author  xuxianchen
 * Package com.xxc.stream.retailersv1
 * Date  2025/8/18 8:46
 * description: mysql db cdc to kafka realtime_db topic Task-01
 * @author 55316
 */
public class DbusCdc2DimHbaseAnd2DbKafka {

    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //所有表cdc
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial(),"52100-52199"
        );

        // 配置表cdc
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial(),"52200-52299"
        );

        //添加所有表
        DataStreamSource<String> cdcDbMainStream = env.fromSource(
                mySQLDbMainCdcSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_cdc_main_source");

        //添加配置表
        DataStreamSource<String> cdcDbDimStream = env.fromSource(
                mySQLCdcDimConfSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_cdc_dim_conf_source");

        //所有表转换
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        //所有表写入kafka
        cdcDbMainStreamMap.map(JSONObject::toString)
                .sinkTo(
                        KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC)
                )
                .uid("mysql_cdc_to_kafka_topic")
                .name("mysql_cdc_to_kafka_topic");

        cdcDbMainStream.print("main  ->");

        //配置表转换
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);

        //处理配置表json串,op=d 删除,op=c 创建,op=u 更新,op=r 读取
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

        cdcDbDimStreamMapCleanColumn.print("cdcDbDimStreamMapCleanColumn ->");

        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(
                new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE)
                ).uid("map_create_hbase_dim_table")
                 .name("map_create_hbase_dim_table");

        cdcDbDimStream.print("dim   ->");
        tpDS.print("aa   ===>");
//
        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);

        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));


        env.disableOperatorChaining();
        env.execute();
    }
}