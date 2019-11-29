package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.input.mysql_binlog.manager.TableManager;

import java.util.Collections;
import java.util.List;

public class PositionHandler implements BinlogEventHandler {
    private final MysqlBinlogManager binlogManager;

    public PositionHandler(MysqlBinlogManager binlogManager) {
        this.binlogManager = binlogManager;
    }

    public List<String> handle(Event event){
        EventHeaderV4 header = event.getHeader();
        if (header.getEventType() == EventType.ROTATE){
            RotateEventData rotateEvent = event.getData();
            this.binlogManager.setBinlogFilename(rotateEvent.getBinlogFilename());
        }
        this.binlogManager.setBinlogPosition(header.getNextPosition());
        return Collections.emptyList();
    }
}
