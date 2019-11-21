package org.embulk.input.mysql_binlog;

public class MysqlBinlogUtil {
    public static String getDeleteFlagName(PluginTask task){
        return task.getMetadataPrefix() + "delete_flag";
    }

    public static String getUpdateAtColumnName(PluginTask task){
        return task.getMetadataPrefix() + "updated_at";
    }
}
