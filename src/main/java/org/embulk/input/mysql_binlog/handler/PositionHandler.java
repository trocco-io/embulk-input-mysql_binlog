package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.google.common.annotations.VisibleForTesting;
import org.embulk.input.mysql_binlog.PluginTask;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

        if (isFinish(this.binlogManager.getTask(), this.binlogManager.getBinlogFilename(), this.binlogManager.getBinlogPosition())){
            // Create new thread to prevent deadlock during disconnecting
            this.binlogManager.setIsConnecting(false);
            MysqlBinlogManager manager = this.binlogManager;
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    manager.disconnect();
                }
            });


        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    public boolean isFinish(PluginTask task, String filename, long position){
        if (task.getToBinlogFilename().isPresent() && task.getToBinlogPosition().isPresent()){
            if (task.getToBinlogFilename().get().equals(filename) && task.getToBinlogPosition().get() == position){
                return true;
            }else if(task.getToBinlogFilename().isPresent() && !task.getToBinlogPosition().isPresent()){
                return task.getToBinlogFilename().get().equals(filename);
            }
        }
        return false;
    }
}
