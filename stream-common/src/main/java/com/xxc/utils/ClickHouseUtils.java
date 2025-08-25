package com.xxc.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * ClickHouse 写入工具（Flink JDBC Sink）
 * 依赖：
 *  - flink-connector-jdbc
 *  - clickhouse-jdbc
 */
public class ClickHouseUtils {

    private static String cfg(String key, String def) {
        try {
            String v = com.xxc.utils.ConfigUtils.getString(key);
            return (v == null || v.trim().isEmpty()) ? def : v;
        } catch (Throwable t) {
            return def;
        }
    }

    /** 从配置拼接 JDBC URL：jdbc:clickhouse://host:port/db?xxx */
    public static String buildJdbcUrl() {
        final String host   = cfg("clickhouse.host",   "localhost");
        final String port   = cfg("clickhouse.port",   "8123");
        final String db     = cfg("clickhouse.database","default");
        final boolean ssl   = Boolean.parseBoolean(cfg("clickhouse.ssl", "false"));
        final String extra  = cfg("clickhouse.properties", ""); // 形如："compress=true&max_execution_time=60"

        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:clickhouse://").append(host).append(":").append(port).append("/").append(db)
          .append("?charset=").append(StandardCharsets.UTF_8.name());
        if (!extra.isEmpty()) sb.append("&").append(extra);
        if (ssl) sb.append("&ssl=true");
        // socket 超时（毫秒）
        String socketTimeout = cfg("clickhouse.socketTimeoutMs", "600000");
        if (!socketTimeout.isEmpty()) sb.append("&socket_timeout=").append(socketTimeout);
        return sb.toString();
    }

    /** 构建 Flink JDBC 连接配置 */
    public static JdbcConnectionOptions buildConnOptions() {
        String url      = buildJdbcUrl();
        String user     = cfg("clickhouse.username", "");
        String password = cfg("clickhouse.password", "");
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder b =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver");
        if (!user.isEmpty())     b.withUsername(user);
        if (!password.isEmpty()) b.withPassword(password);
        return b.build();
    }

    /** 构建 Flink JDBC 批量执行配置 */
    public static JdbcExecutionOptions buildExecOptions() {
        int batchSize   = Integer.parseInt(cfg("clickhouse.batch.size", "50"));
        int batchMs     = Integer.parseInt(cfg("clickhouse.batch.interval.ms", "1000"));
        int maxRetries  = Integer.parseInt(cfg("clickhouse.max.retries", "3"));
        return JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(batchMs)
                .withMaxRetries(maxRetries)
                .build();
    }

    /**
     * 构建一个通用的 ClickHouse Sink
     * @param insertSql 形如：INSERT INTO db.table(col1,col2,...) VALUES(?,?,...)
     * @param binder    如何把对象 T 绑定到 PreparedStatement
     */
    public static <T> SinkFunction<T> buildSink(String insertSql, JdbcStatementBuilder<T> binder) {
        return JdbcSink.sink(
                insertSql,
                binder,
                buildExecOptions(),
                buildConnOptions()
        );
    }
}
