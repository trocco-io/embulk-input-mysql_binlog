package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.embulk.input.mysql_binlog.manager.TableManager;
import org.embulk.input.mysql_binlog.model.Cell;
import org.embulk.input.mysql_binlog.model.Row;
import org.embulk.input.mysql_binlog.model.Table;

import java.util.Collections;
import java.util.List;

public class InsertEventHandler implements BinlogEventHandler {
    private final TableManager tableManager;

    public InsertEventHandler(TableManager tableManager){
        this.tableManager = tableManager;
    }

    @Override
    public List<String> handle(Event event) {
        WriteRowsEventData writeEvent = event.getData();
        Table table = tableManager.getTableInfo(writeEvent.getTableId());
        if (!table.getTableName().equals(tableManager.getTargetTableName())){
            return Collections.emptyList();
        }
        List<Row> rows = table.convertRows(writeEvent.getRows());
        for (Row row: rows) {
            for (Cell cell : row.getCells()) {
                System.out.println(cell.getColumn().getName());
                System.out.println(cell.getValueWithString());
            }
        }
        return Collections.emptyList();
    }
}
