package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.event.EventType;
import lombok.Getter;
import org.embulk.input.mysql_binlog.*;
import org.embulk.input.mysql_binlog.handler.*;
import org.embulk.input.mysql_binlog.model.*;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


public class MysqlBinlogManager {
    private final Logger logger = LoggerFactory.getLogger(MysqlBinlogManager.class);
    private final MysqlBinlogEventHandler handler = new MysqlBinlogEventHandler();
    private final PluginTask task;
    private final PageBuilder pageBuilder;
    @Getter
    private final TableManager tableManager;
    private final MysqlBinlogClient client;
    private final EmbulkPage embulkPage;

    public MysqlBinlogManager(PluginTask task, PageBuilder pageBuilder, Schema schema){
        this.task = task;
        this.pageBuilder = pageBuilder;

        this.setBinlogFilename(task.getFromBinlogFilename());
        this.setBinlogPosition(task.getFromBinlogPosition());
        this.setCurrentDdl(task.getDdl());
        DbInfo dbInfo = MysqlBinlogClient.convertTaskToDbInfo(task);

        this.embulkPage = new EmbulkPage(task, pageBuilder, schema);
        this.tableManager = new TableManager(dbInfo, task);
        // TODO: parse DDL to normalize table name
        this.tableManager.migrate(task.getDdl());

        this.client = new MysqlBinlogClient(dbInfo, getBinlogFilename(), getBinlogPosition());
        this.registerHandler();
        this.client.registerEventListener(handler);
    }

    public void setIsConnecting(boolean isConnecting){
        this.client.setConnecting(isConnecting);
    }

    public boolean getIsConnecting(){
        return this.client.getConnecting();
    }


    public void addRows(List<Row> rows, boolean deleteFlag){
        this.embulkPage.addRecords(rows, deleteFlag);
    }

    public void flush(){
        this.embulkPage.flush();
    }

    public void connect(){
        try {
            this.client.connect();
        } catch (Exception e){
            // TODO: handle error proper
            System.out.println(e.getMessage());
        }
        this.pageBuilder.finish();
    }

    public void disconnect(){
        try {
            this.client.disconnect();
        } catch (IOException e){
            System.out.println(e.getMessage());
        }
    }

    public void setBinlogFilename(String binlogFilename){
        MysqlBinlogPosition.setCurrentBinlogFilename(binlogFilename);
    }

    public String getBinlogFilename() {
        return MysqlBinlogPosition.getCurrentBinlogFilename();
    }

    public void setBinlogPosition(long binlogPosition){
        MysqlBinlogPosition.setCurrentBinlogPosition(binlogPosition);
    }

    public long getBinlogPosition() {
        return MysqlBinlogPosition.getCurrentBinlogPosition();
    }

    public void setCurrentDdl(String ddl){
        MysqlBinlogPosition.setCurrentDdl(ddl);
    }

    public PluginTask getTask(){
        return this.task;
    }

    private void registerHandler(){
        this.handler.registerHandler(new InsertEventHandler(this.tableManager, this), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS);
        this.handler.registerHandler(new UpdateEventHandler(this.tableManager, this), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS);
        this.handler.registerHandler(new DeleteEventHandler(this.tableManager, this), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS);
        this.handler.registerHandler(new TableMapEventHandler(this.tableManager), EventType.TABLE_MAP);
        this.handler.registerHandler(new QueryEventHandler(this.tableManager), EventType.QUERY);
        this.handler.registerPositionHandler(new PositionHandler(this));
    }
}
