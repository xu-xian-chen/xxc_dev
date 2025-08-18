package com.xxc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class HBaseConnectionTest {
    public static void main(String[] args) {
        // 替换成你自己的 zk 地址和端口
        String zkQuorum = "cdh01:2181,cdh02:2181,cdh03:2181";
        String znodeParent = "/hbase"; // CDH 默认可能是 /hbase-unsecure，根据你集群确认

        Connection connection = null;
        Admin admin = null;

        try {
            // 1. 创建配置
            Configuration config = HBaseConfiguration.create();
            config.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
            config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);

            // 2. 建立连接
            connection = ConnectionFactory.createConnection(config);
            System.out.println("✅ 成功连接到 HBase!");

            // 3. 获取 Admin
            admin = connection.getAdmin();

            // 4. 列出所有表
            TableName[] tableNames = admin.listTableNames();
            System.out.println("📋 集群表清单:");
            for (TableName tn : tableNames) {
                System.out.println(" - " + tn.getNameAsString());
            }

            // 5. 测试读取一张表（可选）
            String testTable = "realtime_v2:dim_activity_info"; // 你要确认的表
            if (admin.tableExists(TableName.valueOf(testTable))) {
                try (Table table = connection.getTable(TableName.valueOf(testTable))) {
                    Get get = new Get("test".getBytes());
                    Result result = table.get(get);
                    System.out.println("🔍 读取表 " + testTable + "，结果是否为空: " + result.isEmpty());
                }
            } else {
                System.out.println("⚠️ 表 " + testTable + " 不存在!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ignore) {}
        }
    }
}
