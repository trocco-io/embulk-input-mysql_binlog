package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.google.common.annotations.VisibleForTesting;
import org.embulk.input.mysql_binlog.PluginTask;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class PositionHandler implements BinlogEventHandler {
    private final Logger logger = LoggerFactory.getLogger(PositionHandler.class);
    private final MysqlBinlogManager binlogManager;

    private boolean afterInitialBinlogFile = false;

    public PositionHandler(MysqlBinlogManager binlogManager) {
        this.binlogManager = binlogManager;
    }

    public boolean shouldHandle(Event event) {
        if (afterInitialBinlogFile){
            return true;
        }else{
            EventHeaderV4 header = event.getHeader();
            return header.getPosition() >= binlogManager.getInitialBinlogPosition();
        }
    }

    public List<String> handle(Event event) {
        EventHeaderV4 header = event.getHeader();
        if (header.getEventType() == EventType.ROTATE) {
            RotateEventData rotateEvent = event.getData();
            String binlogFilename = rotateEvent.getBinlogFilename();
            if (compareBinlogFilename(binlogFilename, this.binlogManager.getInitialBinlogFilename()) > 0) {
                this.afterInitialBinlogFile = true;
            }
            this.binlogManager.setBinlogFilename(rotateEvent.getBinlogFilename());
            logger.info("binlog file: {}", rotateEvent.getBinlogFilename());
            // flush embulk page
            this.binlogManager.flush();
        }
        this.binlogManager.setBinlogPosition(header.getNextPosition());

        if (isFinish(this.binlogManager.getTask(), this.binlogManager.getBinlogFilename(),
                this.binlogManager.getBinlogPosition())) {
            // Create new thread to prevent deadlock during disconnecting
            // ref: shyiko/mysql-binlog-connector-java#230
            this.binlogManager.setIsConnecting(false);
            logger.info("disconnect");
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
    public boolean isFinish(PluginTask task, String filename, long position) {
        if (task.getToBinlogFilename().isPresent() && task.getToBinlogPosition().isPresent()) {
            if (task.getToBinlogFilename().get().equals(filename) && task.getToBinlogPosition().get() == position) {
                return true;
            } else if (task.getToBinlogFilename().isPresent() && !task.getToBinlogPosition().isPresent()) {
                return task.getToBinlogFilename().get().equals(filename);
            }
        }
        return false;
    }

    public int compareBinlogFilename(String s1, String s2) {
        Pattern pattern = Pattern.compile("\\d+$");

        Matcher matcher1 = pattern.matcher(s1);
        Matcher matcher2 = pattern.matcher(s2);

        if (matcher1.find() && matcher2.find()) {
            String number1 = matcher1.group();
            String number2 = matcher2.group();

            return Integer.compare(Integer.parseInt(number1), Integer.parseInt(number2));
        }

        return 0;
    }
}
