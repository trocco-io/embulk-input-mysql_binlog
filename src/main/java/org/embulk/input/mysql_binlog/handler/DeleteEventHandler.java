package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.input.mysql_binlog.manager.TableManager;
import org.embulk.input.mysql_binlog.model.Cell;
import org.embulk.input.mysql_binlog.model.Row;
import org.embulk.input.mysql_binlog.model.Table;

import java.util.Collections;
import java.util.List;

public class DeleteEventHandler implements BinlogEventHandler {
    private final TableManager tableManager;
    private final MysqlBinlogManager binlogManager;

    public DeleteEventHandler(TableManager tableManager, MysqlBinlogManager binlogManager){
        this.tableManager = tableManager;
        this.binlogManager = binlogManager;
    }

    @Override
    public List<String> handle(Event event) {
        DeleteRowsEventData deleteEvent = event.getData();
        Table table = tableManager.getTableInfo(deleteEvent.getTableId());
        if (!table.getTableName().equals(tableManager.getTargetTableName())){
            return Collections.emptyList();
        }
        List<Row> rows = table.convertRows(deleteEvent.getRows());
        this.binlogManager.addRows(rows);
        return Collections.emptyList();
    }
}
