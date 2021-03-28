package org.embulk.input.mysql_binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.embulk.input.mysql_binlog.handler.BinlogEventHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlBinlogEventHandler implements BinlogEventHandler{
    private Map<EventType, BinlogEventHandler> handlers = new HashMap<>();
    private BinlogEventHandler positionHandler;

    public void registerHandler(BinlogEventHandler handler, EventType... eventTypes){
        for (EventType eventType: eventTypes){
            handlers.put(eventType, handler);
        }
    }


    public void registerPositionHandler(BinlogEventHandler positionHandler){
        this.positionHandler = positionHandler;
    }

    public List<String> handle(Event event){
        BinlogEventHandler handler = handlers.get(event.getHeader().getEventType());
        if (handler != null){
            handler.handle(event);
        }
        positionHandler.handle(event);
        return Collections.emptyList();
    }

}
