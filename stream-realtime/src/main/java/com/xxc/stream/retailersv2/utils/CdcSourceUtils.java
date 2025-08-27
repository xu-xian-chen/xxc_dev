package com.xxc.stream.retailersv2.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xxc.utils.ConfigUtils;

import java.util.Properties;

/**
 * @Package com.stream.common.utils.CdcSourceUtils
 * @Author zhou.han
 * @Date 2024/12/17 11:49
 * @description: MySQL Cdc Source
 */
public class CdcSourceUtils {

    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,StartupOptions model){
        return getMySQLCdcSource(database, table, username, pwd, model, "54100-54200");
    }
    
    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,StartupOptions model, String serverId){
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        debeziumProperties.setProperty("snapshot.mode", "schema_only");
        debeziumProperties.setProperty("include.schema.changes", "false");
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");
        return  MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
                .serverId(serverId)
//                .connectionTimeZone(ConfigUtils.getString("mysql.timezone"))、
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .debeziumProperties(debeziumProperties)
                .build();
    }
}