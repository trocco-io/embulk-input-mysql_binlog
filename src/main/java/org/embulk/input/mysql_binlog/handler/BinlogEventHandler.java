package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.List;

public interface BinlogEventHandler {
    List<String> handle(Event event);
}
