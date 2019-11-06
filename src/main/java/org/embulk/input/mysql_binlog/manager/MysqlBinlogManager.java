package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.embulk.input.mysql_binlog.MysqlBinlogParser;
import org.embulk.input.mysql_binlog.PluginTask;
import org.embulk.input.mysql_binlog.handler.DeleteEventHandler;
import org.embulk.input.mysql_binlog.handler.InsertEventHandler;
import org.embulk.input.mysql_binlog.handler.TableMapEventHandler;
import org.embulk.input.mysql_binlog.handler.UpdateEventHandler;
import org.embulk.input.mysql_binlog.model.DbInfo;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

public class MysqlBinlogManager {
    private final MysqlBinlogParser parser = new MysqlBinlogParser();
    private final PluginTask task;
    private final PageBuilder pageBuilder;
    private final Schema schema;
    private TableManager tableManager;
    private DbInfo dbInfo;
    private BinaryLogClient client;

    public MysqlBinlogManager(PluginTask task, PageBuilder pageBuilder, Schema schema){
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;
        this.setDbInfo();
        this.tableManager = new TableManager(this.dbInfo, task.getTable());
        this.registerHandler();
        this.client = this.initClient();
    }

    public void connect(){
        try {
            this.client.setBlocking(false);
            this.client.connect();
        } catch (Exception e){
            // TODO: handle error proper
            System.out.println(e.getMessage());
        }
    }

    private BinaryLogClient initClient(){
        BinaryLogClient client = new BinaryLogClient(this.dbInfo.getHost(), this.dbInfo.getPort(), this.dbInfo.getUser(), this.dbInfo.getPassword());
        client.setBinlogFilename(this.task.getBinlogFilename());
        client.setBinlogPosition(this.task.getBinlogPosition());
        client.registerEventListener(event -> {
            System.out.println(event.getHeader().getEventType());
            parser.handle(event);
        });
        return client;
    }

    private void setDbInfo(){
        this.dbInfo = new DbInfo(task.getHost(), task.getPort(), task.getDatabase(), task.getUser(), task.getPassword());
    }

    private void registerHandler(){
        this.parser.registerHandler(new InsertEventHandler(this.tableManager, this), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS, EventType.PRE_GA_WRITE_ROWS);
        this.parser.registerHandler(new UpdateEventHandler(this.tableManager, this), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS, EventType.PRE_GA_UPDATE_ROWS);
        this.parser.registerHandler(new DeleteEventHandler(this.tableManager, this), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS, EventType.PRE_GA_DELETE_ROWS);
        this.parser.registerHandler(new TableMapEventHandler(this.tableManager), EventType.TABLE_MAP);
    }
}
