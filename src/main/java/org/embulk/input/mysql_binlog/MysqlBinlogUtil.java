package org.embulk.input.mysql_binlog;

import java.util.HashMap;
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
        /** 唯一のインスタンス */
        private static final AtomicLong INSTANCE = new AtomicLong(0);
    }
}
