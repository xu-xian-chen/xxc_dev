package com.xxc.stream.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.xxc.utils.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject, JSONObject> {

    private final String hbaseNameSpace;
    private final String zkHostList;
    private HbaseUtils hbaseUtils;

    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseUtils = new HbaseUtils(zkHostList);
        // ★ 先确保命名空间存在（幂等）
        hbaseUtils.createNamespaceIfAbsent(hbaseNameSpace);
    }

    @Override
    public JSONObject map(JSONObject json) throws Exception {
        final String op = json.getString("op");

        if ("d".equals(op)) {
            // 配置删除：谨慎处理，只删除对应 ns:table
            String beforeTable = json.getJSONObject("before").getString("sink_table");
            if (beforeTable != null && !beforeTable.isEmpty()) {
                hbaseUtils.deleteTable(hbaseNameSpace + ":" + beforeTable);
            }
            return json;
        }

        // r/c/u 都走“确保存在”逻辑
        String afterTable = json.getJSONObject("after").getString("sink_table");
        String sinkFamily = json.getJSONObject("after").getString("sink_family");
        if (sinkFamily == null || sinkFamily.trim().isEmpty()) {
            sinkFamily = "info";
        }

        // 若是更新操作且表名改了，仅创建新表，不自动删除旧表（防数据丢失）
        if ("u".equals(op)) {
            String beforeTable = json.getJSONObject("before").getString("sink_table");
            if (beforeTable != null && !beforeTable.equals(afterTable)) {
                // 新表：确保存在
                if (!hbaseUtils.tableIsExists(hbaseNameSpace + ":" + afterTable)) {
                    hbaseUtils.createTable(hbaseNameSpace, afterTable, sinkFamily);
                }
                return json;
            }
            // 否则 fall through 到通用创建
        }

        // 通用：不存在则创建（带列族）
        if (!hbaseUtils.tableIsExists(hbaseNameSpace + ":" + afterTable)) {
            hbaseUtils.createTable(hbaseNameSpace, afterTable, sinkFamily);
        }
        return json;
    }

    @Override
    public void close() throws Exception {
        // 让工具类自己管理连接/资源，如果你有单独关闭方法可在此调用
        super.close();
    }
}
