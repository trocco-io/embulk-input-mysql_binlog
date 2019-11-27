package org.embulk.input.mysql_binlog;

/**
 * Store MySQL binlog position
 */
public class MysqlBinlogPositionStore {
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

    public void setToBinlogPosition(long binlogPosition){
        toBinlogPosition = binlogPosition;
    }
}
