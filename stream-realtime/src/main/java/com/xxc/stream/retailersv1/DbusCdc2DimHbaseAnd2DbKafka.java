package com.xxc.stream.retailersv1;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.xxc.stream.retailersv1.utils.CdcSourceUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
    private static final String MYSQL_DATABASE = ConfigUtils.getString("mysql.database");
    private static final String MYSQL_USER = ConfigUtils.getString("mysql.user");
    private static final String MYSQL_PWD = ConfigUtils.getString("mysql.pwd");
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        // 读取配置库的变化binlog
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_conf_source");


        cdcDbMainStream.print("main  ->");
        cdcDbDimStream.print("dim   ->");
        env.execute();
    }
}