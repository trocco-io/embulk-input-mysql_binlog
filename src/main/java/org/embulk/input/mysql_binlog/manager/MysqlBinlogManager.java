package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import org.embulk.input.mysql_binlog.*;
import org.embulk.input.mysql_binlog.handler.*;
import org.embulk.input.mysql_binlog.model.Cell;
import org.embulk.input.mysql_binlog.model.Column;
import org.embulk.input.mysql_binlog.model.DbInfo;
import org.embulk.input.mysql_binlog.model.Row;
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
    private Column updatedAtColumn;
    private String binlogFilename;
    private long binlogPosition;

    public MysqlBinlogManager(PluginTask task, PageBuilder pageBuilder, Schema schema){
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;
        this.setDbInfo();
        this.tableManager = new TableManager(this.dbInfo, task.getTable());
        this.registerHandler();
        this.client = this.initClient();
        this.deleteFlagColumn = new Column(MysqlBinlogUtil.getDeleteFlagName(task), ColumnType.TINY, JDBCType.BOOLEAN);
        this.updatedAtColumn = new Column(MysqlBinlogUtil.getUpdateAtColumnName(task), ColumnType.TIMESTAMP_V2, JDBCType.TIMESTAMP);
        this.binlogFilename = task.getBinlogFilename();
        this.binlogPosition = task.getBinlogPosition();
    }
    public void addRows(List<Row> rows, boolean deleteFlag){
        for (Row row: rows) {
            List<Cell> cells = row.getCells();
            Cell deleteFlagCell = new Cell(deleteFlag, deleteFlagColumn);
            cells.add(deleteFlagCell);
            Timestamp now = new Timestamp(System.currentTimeMillis());
            // TODO: use default time stamp format
            String ts = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz").format(now);
            Cell updatedAtCell = new Cell(ts, updatedAtColumn);
            cells.add(updatedAtCell);
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

    public void setBinlonFilename(String binlogFilename){
        this.binlogFilename = binlogFilename;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public void setBinlogPosition(long binglogPosition){
        this.binlogPosition = binglogPosition;
    }

    public long getBinlogPosition() {
        return binlogPosition;
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
        this.parser.registerHandler(new InsertEventHandler(this.tableManager, this), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS);
        this.parser.registerHandler(new UpdateEventHandler(this.tableManager, this), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS);
        this.parser.registerHandler(new DeleteEventHandler(this.tableManager, this), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS);
        this.parser.registerHandler(new TableMapEventHandler(this.tableManager), EventType.TABLE_MAP);
        this.parser.registerHandler(new RotateEventHandler(this.tableManager,this), EventType.ROTATE);
    }
}
