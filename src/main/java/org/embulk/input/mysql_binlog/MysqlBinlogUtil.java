package org.embulk.input.mysql_binlog;

import java.util.concurrent.atomic.AtomicLong;

public class MysqlBinlogUtil {
    public static String getDeleteFlagName(PluginTask task){
        return task.getMetadataPrefix() + "deleted";
    }

    public static String getFetchedAtName(PluginTask task){
        return task.getMetadataPrefix() + "fetched_at";
    }

    public static String getSeqName(PluginTask task){
        return task.getMetadataPrefix() + "seq";
    }

    public static AtomicLong getSeqCounter(){
        return SeqCounterHolder.INSTANCE;
    }

    public static class SeqCounterHolder {
        private static final AtomicLong INSTANCE = new AtomicLong(0);
    }
    public static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte aByte : bytes) {
            result.append(String.format("%02x", aByte));
            // upper case
            // result.append(String.format("%02X", aByte));
        }
        return result.toString();
    }
}
