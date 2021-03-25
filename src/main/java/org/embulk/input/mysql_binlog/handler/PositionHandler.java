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
        System.out.println("PositionHandler start");
        EventHeaderV4 header = event.getHeader();
        if (header.getEventType() == EventType.ROTATE){
            RotateEventData rotateEvent = event.getData();
            this.binlogManager.setBinlogFilename(rotateEvent.getBinlogFilename());
        }
        System.out.println("PositionHandler 1");
        this.binlogManager.setBinlogPosition(header.getNextPosition());
        System.out.println("PositionHandler 2");

        if (isFinish(this.binlogManager.getTask(), this.binlogManager.getBinlogFilename(), this.binlogManager.getBinlogPosition())){
            System.out.println("PositionHandler 3");
            // Create new thread to prevent deadlock during disconnecting
            // ref: shyiko/mysql-binlog-connector-java#230
            this.binlogManager.setIsConnecting(false);
            System.out.println("disconnect");
            MysqlBinlogManager manager = this.binlogManager;
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    manager.disconnect();
                }
            });


        }
        System.out.println("PositionHandler end");
        return Collections.emptyList();
    }

    @VisibleForTesting
    public boolean isFinish(PluginTask task, String filename, long position){
        System.out.println("PositionHandler isFinish");
        if (task.getToBinlogFilename().isPresent() && task.getToBinlogPosition().isPresent()){
            System.out.println("PositionHandler isFinish 1");
            if (task.getToBinlogFilename().get().equals(filename) && task.getToBinlogPosition().get() == position){
                System.out.println("PositionHandler isFinish 2");
                return true;
            }else if(task.getToBinlogFilename().isPresent() && !task.getToBinlogPosition().isPresent()){
                System.out.println("PositionHandler isFinish 3");
                return task.getToBinlogFilename().get().equals(filename);
            }
        }
        return false;
    }
}
