package org.embulk.input.mysql_binlog;

import org.embulk.input.mysql_binlog.model.Cell;
import org.embulk.input.mysql_binlog.model.Row;

// TODO: is this really need this layer?
public class MysqlBinlogAccessor {
    private final Row row;

    public MysqlBinlogAccessor(final Row row){
        this.row = row;
    }

    public String get(String name){
        for (Cell cell:
             this.row.getCells()) {
            if (cell.getColumn().getName().equals(name)){
                return cell.getValueWithString();
            }
        }
        return null;
    }
}
