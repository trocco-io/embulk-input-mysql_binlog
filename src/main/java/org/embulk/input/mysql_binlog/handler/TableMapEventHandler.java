package org.embulk.input.mysql_binlog.handler;


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import org.embulk.input.mysql_binlog.manager.TableManager;
import org.embulk.input.mysql_binlog.model.Column;
import org.embulk.input.mysql_binlog.model.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableMapEventHandler implements BinlogEventHandler {
    private TableManager tableManager;

    public TableMapEventHandler(TableManager tableManager){
        this.tableManager = tableManager;
    }

    @Override
    public List<String> handle(Event event) {
        TableMapEventData eventData = event.getData();
        tableManager.setTableInfo(eventData);
        Table table = new Table(eventData.getDatabase(), eventData.getTable());
        if (!eventData.getTable().equals(tableManager.getTargetTableName())){
            return Collections.emptyList();
        }

        byte[] columnMeta = eventData.getColumnTypes();

        // TODO: use stream zip
        List<Column> columns = new ArrayList<>();
        for(int i = 0; i < columnMeta.length; i++){
            Column column = new Column();
            column.setColumnType(ColumnType.byCode(columnMeta[i] & 0xFF));
            columns.add(column);
        }
        System.out.println(columns);
        TableMapEventMetadata eventMeta = eventData.getEventMetadata();
        List<String> columnNames = eventMeta.getColumnNames();
        System.out.println(columnNames);
        for (int i=0; i < columnNames.size(); i++){
            columns.get(i).setName(columnNames.get(i));
        }
        System.out.println(columns);
        table.setColumns(columns);
        this.tableManager.putTableInfo(eventData.getTableId(), table);
        return Collections.emptyList();
    }
}
