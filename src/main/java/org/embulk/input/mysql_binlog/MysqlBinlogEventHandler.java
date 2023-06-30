package org.embulk.input.mysql_binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.embulk.input.mysql_binlog.handler.BinlogEventHandler;
import org.embulk.input.mysql_binlog.handler.PositionHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlBinlogEventHandler implements BinlogEventHandler {
    private final Map<EventType, BinlogEventHandler> handlers = new HashMap<>();
    private PositionHandler positionHandler;

    public void registerHandler(BinlogEventHandler handler, EventType... eventTypes) {
        for (EventType eventType : eventTypes) {
            handlers.put(eventType, handler);
        }
    }

    public void registerPositionHandler(PositionHandler positionHandler) {
        this.positionHandler = positionHandler;
    }

    public boolean shouldHandleEvent(Event event) {
        return positionHandler.shouldHandle(event);
    }

    public List<String> handle(Event event) {
        if (shouldHandleEvent(event)){
            BinlogEventHandler handler = handlers.get(event.getHeader().getEventType());
            if (handler != null) {
                handler.handle(event);
            }
        }
        positionHandler.handle(event);
        return Collections.emptyList();
    }

}
