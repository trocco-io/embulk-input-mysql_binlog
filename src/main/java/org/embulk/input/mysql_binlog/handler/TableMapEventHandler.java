package org.embulk.input.mysql_binlog.handler;


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import org.embulk.input.mysql_binlog.manager.TableManager;

import java.util.Collections;
import java.util.List;

// update table id and table name mapping
public class TableMapEventHandler implements BinlogEventHandler {
    private TableManager tableManager;

    public TableMapEventHandler(TableManager tableManager){
        this.tableManager = tableManager;
    }

    @Override
    public List<String> handle(Event event) {
        TableMapEventData eventData = event.getData();
        if (!eventData.getDatabase().equals(tableManager.getDatabaseName())){
            return Collections.emptyList();
        }

        if (!eventData.getTable().equals(tableManager.getTableName())){
            return Collections.emptyList();
        }

        tableManager.setTableInfo(eventData);
        return Collections.emptyList();
    }
}
