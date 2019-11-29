package org.embulk.input.mysql_binlog.model;

/**
 * Store MySQL binlog position
 */
public class MysqlBinlogPosition {
    private static String currentBinlogFilename;
    private static long currentBinlogPosition;
    private static String toBinlogFilename;
    private static long toBinlogPosition;

    public static String getCurrentBinlogFilename(){
        return currentBinlogFilename;
    }

    public static void setCurrentBinlogFilename(String binlogFilename){
        currentBinlogFilename = binlogFilename;
    }

    public static long getCurrentBinlogPosition() {
        return currentBinlogPosition;
    }

    public static void setCurrentBinlogPosition(long binlogPosition) {
        currentBinlogPosition = binlogPosition;
    }

    public static String getToBinlogFilename() {
        return toBinlogFilename;
    }

    public static void setToBinlogFilename(String binlogFilename){
        toBinlogFilename = binlogFilename;
    }

    public static long getToBinlogPosition(){
        return toBinlogPosition;
    }

    public static void setToBinlogPosition(long binlogPosition){
        toBinlogPosition = binlogPosition;
    }
}
