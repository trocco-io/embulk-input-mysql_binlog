package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.input.mysql_binlog.manager.TableManager;
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

        if (!BinlogEventHandlerHelper.shouldHandle(table,
                tableManager.getTable(), tableManager.getDatabase())){
            return Collections.emptyList();
        }

        // Old row needs to delete in case where the primary key is changed.
        // getKey => before
        // getValue => after
        List<Serializable[]> rawOldRaw = updateEvent.getRows().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        List<Serializable[]> rawNewRaw = updateEvent.getRows().stream().map(Map.Entry::getValue).collect(Collectors.toList());
        List<Row> oldRows = table.convertRows(rawOldRaw);
        List<Row> newRows = table.convertRows(rawNewRaw);
        this.binlogManager.addRows(oldRows, true);
        this.binlogManager.addRows(newRows, false);
        return Collections.emptyList();
    }
}
