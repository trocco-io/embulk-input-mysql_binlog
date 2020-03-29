package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import org.embulk.input.mysql_binlog.*;
import org.embulk.input.mysql_binlog.handler.*;
import org.embulk.input.mysql_binlog.model.*;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class MysqlBinlogManager {
    private final MysqlBinlogParser parser = new MysqlBinlogParser();
    private final PluginTask task;
    private final PageBuilder pageBuilder;
    private final Schema schema;
    private TableManager tableManager;
    private DbInfo dbInfo;
    private BinaryLogClient client;
    private Column deleteFlagColumn;
    private Column fetchedAtColumn;

    public MysqlBinlogManager(PluginTask task, PageBuilder pageBuilder, Schema schema){
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;
        this.setDbInfo();
        this.tableManager = new TableManager(this.dbInfo, task);
        this.registerHandler();
        this.deleteFlagColumn = new Column(MysqlBinlogUtil.getDeleteFlagName(task), ColumnType.TINY, JDBCType.BOOLEAN);
        this.fetchedAtColumn = new Column(MysqlBinlogUtil.getFetchedAtName(task), ColumnType.TIMESTAMP_V2, JDBCType.TIMESTAMP);
        this.setBinlogFilename(task.getFromBinlogFilename());
        this.setBinlogPosition(task.getFromBinlogPosition());
        this.client = this.initClient();
    }
    public void addRows(List<Row> rows, boolean deleteFlag){
        for (Row row: rows) {
            List<Cell> cells = row.getCells();
            if (task.getEnableMetadataDeleted()){
                Cell deleteFlagCell = new Cell(deleteFlag, deleteFlagColumn);
                cells.add(deleteFlagCell);
            }

            if (task.getEnableMetadataFetchedAt()){
                Timestamp now = new Timestamp(System.currentTimeMillis());
                // TODO: use default time stamp format
                // yyyy-MM-dd HH:mm:ssz would be good
                String ts = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz").format(now);
                Cell fetchedAtCell = new Cell(ts, fetchedAtColumn);
                cells.add(fetchedAtCell);
            }
            Row newRow = new Row(cells);

            this.schema.visitColumns(new MysqlBinlogColumnVisitor(new MysqlBinlogAccessor(newRow), this.pageBuilder, this.task));
            this.pageBuilder.addRecord();
        }
    }

    public void addRows(List<Row> rows){
        for (Row row: rows) {
            this.schema.visitColumns(new MysqlBinlogColumnVisitor(new MysqlBinlogAccessor(row), this.pageBuilder, this.task));
            this.pageBuilder.addRecord();
        }
    }

    public void connect(){
        try {
            this.client.setBlocking(false);
            this.client.connect();
        } catch (Exception e){
            // TODO: handle error proper
            System.out.println(e.getMessage());
        }
        this.pageBuilder.finish();
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
        return MysqlBinlogPosition.getToBinlogPosition();
    }

    private BinaryLogClient initClient(){
        BinaryLogClient client = new BinaryLogClient(this.dbInfo.getHost(), this.dbInfo.getPort(), this.dbInfo.getUser(), this.dbInfo.getPassword());
        client.setBinlogFilename(this.getBinlogFilename());
        client.setBinlogPosition(this.getBinlogPosition());
        client.registerEventListener(event -> {
            // TODO: add filter

            // TODO: pass client and handle binlog position and disconnect
            parser.handle(event);
        });
        return client;
    }

    private void setDbInfo(){
        this.dbInfo = new DbInfo(task.getHost(), task.getPort(), task.getDatabase(), task.getUser(), task.getPassword());
    }

    private void registerHandler(){
        this.parser.registerHandler(new InsertEventHandler(this.tableManager, this), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS);
        this.parser.registerHandler(new UpdateEventHandler(this.tableManager, this), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS);
        this.parser.registerHandler(new DeleteEventHandler(this.tableManager, this), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS);
        this.parser.registerHandler(new TableMapEventHandler(this.tableManager), EventType.TABLE_MAP);
        this.parser.registerPositionHandler(new PositionHandler(this));
    }
}
