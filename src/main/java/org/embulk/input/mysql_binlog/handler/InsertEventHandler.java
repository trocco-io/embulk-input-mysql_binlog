package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.input.mysql_binlog.manager.TableManager;
import org.embulk.input.mysql_binlog.model.Row;
import org.embulk.input.mysql_binlog.model.Table;

import java.util.Collections;
import java.util.List;

public class InsertEventHandler implements BinlogEventHandler {
    private final TableManager tableManager;
    private final MysqlBinlogManager binlogManager;

    public InsertEventHandler(TableManager tableManager, MysqlBinlogManager binlogManager){
        this.tableManager = tableManager;
        this.binlogManager = binlogManager;
    }

    @Override
    public List<String> handle(Event event) {
        WriteRowsEventData writeEvent = event.getData();
        // TODO: get table name by name
        Table table = tableManager.getTableInfo(writeEvent.getTableId());

        // TODO: this should be handle by binlog manager
        if (!table.getTableName().equals(tableManager.getTargetTableName())){
            return Collections.emptyList();
        }
        List<Row> rows = table.convertRows(writeEvent.getRows());
        this.binlogManager.addRows(rows, false);
        return Collections.emptyList();
    }
}
