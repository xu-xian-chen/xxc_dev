package com.xxc.stream.retailersv1.utils;

import com.xxc.utils.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Package com.stream.utils.HiveCatalogUtils
 * @Author zhou.han
 * @Date 2024/12/30 08:57
 * @description: hive catalog utils
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}
