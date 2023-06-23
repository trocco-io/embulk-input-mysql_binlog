package org.embulk.input.mysql_binlog.handler;

import org.embulk.input.mysql_binlog.model.Table;

public class BinlogEventHandlerHelper {
    public static boolean shouldHandle(Table table, String tableName, String databaseName) {
        if (table == null) {
            return false;
        }

        return table.getTableName().equals(tableName)
                && table.getDatabaseName().equals(databaseName);
    }
}
