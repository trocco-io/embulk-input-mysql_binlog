package org.embulk.input.mysql_binlog;

import java.util.HashMap;

public class MysqlBinlogUtil {
    public static String getDeleteFlagName(PluginTask task){
        return task.getMetadataPrefix() + "delete_flag";
    }

    public static String getUpdateAtColumnName(PluginTask task){
        return task.getMetadataPrefix() + "updated_at";
    }

    // separate file?
    // make sure this is thread safe
    // reject old binlog info is stored
    private static final HashMap<String, String> props = new HashMap<String, String>();

    public static String setVal(String key, String value) {
        return props.put(key, value);
    }

    public static String getVal(String key) {
        return props.get(key);
    }
}
