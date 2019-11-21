package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.input.mysql_binlog.manager.TableManager;
import org.embulk.input.mysql_binlog.model.Cell;
import org.embulk.input.mysql_binlog.model.Row;
import org.embulk.input.mysql_binlog.model.Table;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateEventHandler implements BinlogEventHandler {
    private final TableManager tableManager;
    private final MysqlBinlogManager binlogManager;

    public UpdateEventHandler(TableManager tableManager, MysqlBinlogManager binlogManager) {
        this.tableManager = tableManager;
        this.binlogManager = binlogManager;
    }

    @Override
    public List<String> handle(Event event) {
        UpdateRowsEventData updateEvent = event.getData();
        Table table = tableManager.getTableInfo(updateEvent.getTableId());
        if (!table.getTableName().equals(tableManager.getTargetTableName())){
            return Collections.emptyList();
        }

        // getKey => before
        // getValue => after
        List<Serializable[]> rawRaw = updateEvent.getRows().stream().map(Map.Entry::getValue).collect(Collectors.toList());
        List<Row> rows = table.convertRows(rawRaw);
        this.binlogManager.addRows(rows, false);
        return Collections.emptyList();
    }
}
