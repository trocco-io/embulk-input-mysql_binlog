package org.embulk.input.mysql_binlog.handler;


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import org.embulk.input.mysql_binlog.manager.TableManager;

import java.util.Collections;
import java.util.List;

public class TableMapEventHandler implements BinlogEventHandler {
    private TableManager tableManager;

    public TableMapEventHandler(TableManager tableManager){
        this.tableManager = tableManager;
    }

    @Override
    public List<String> handle(Event event) {
        System.out.println("tableMapEventhandler Start");
        TableMapEventData eventData = event.getData();
        System.out.println("event.getData() END");
        System.out.println("setTableInfo Start");
        tableManager.setTableInfo(eventData);
        System.out.println("setTableInfo END");
        System.out.println("tableMapEventhandler END");
        return Collections.emptyList();
    }
}
