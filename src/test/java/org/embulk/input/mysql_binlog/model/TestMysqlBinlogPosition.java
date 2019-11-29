package org.embulk.input.mysql_binlog.model;

import org.embulk.input.mysql_binlog.MysqlBinlogUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMysqlBinlogPosition {
    @Test
    public void testSetAndGet() {
        String currentBinlogFilename = "mysql-bin.000001";
        long currentBinlogPosition = 1;
        String toBinlogFilename = "mysql-bin.999999";
        long toBinlogPosition = 999999;

        MysqlBinlogPosition.setCurrentBinlogFilename(currentBinlogFilename);
        MysqlBinlogPosition.setToBinlogFilename(toBinlogFilename);
        MysqlBinlogPosition.setCurrentBinlogPosition(currentBinlogPosition);
        MysqlBinlogPosition.setToBinlogPosition(toBinlogPosition);

        assertEquals(currentBinlogFilename, MysqlBinlogPosition.getCurrentBinlogFilename());
        assertEquals(currentBinlogPosition, MysqlBinlogPosition.getCurrentBinlogPosition());
        assertEquals(toBinlogFilename, MysqlBinlogPosition.getToBinlogFilename());
        assertEquals(toBinlogPosition, MysqlBinlogPosition.getToBinlogPosition());
    }
}
