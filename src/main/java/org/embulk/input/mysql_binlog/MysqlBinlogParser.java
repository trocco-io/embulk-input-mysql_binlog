package org.embulk.input.mysql_binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.embulk.input.mysql_binlog.handler.BinlogEventHandler;

import java.util.HashMap;
import java.util.Map;

public class MysqlBinlogParser {
    private Map<EventType, BinlogEventHandler> handlers = new HashMap<>();

    public void registerHandler(BinlogEventHandler handler, EventType... eventTypes){
        for (EventType eventType: eventTypes){
            handlers.put(eventType, handler);
        }
    }

    public void handle(Event event){
        BinlogEventHandler handler = handlers.get(event.getHeader().getEventType());
        if (handler != null){
            handler.handle(event);
        }
    }
}
