package org.embulk.input.mysql_binlog.model;

import org.embulk.input.mysql_binlog.MysqlBinlogAccessor;
import org.embulk.input.mysql_binlog.MysqlBinlogColumnVisitor;
import org.embulk.input.mysql_binlog.MysqlBinlogUtil;
import org.embulk.input.mysql_binlog.PluginTask;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

import java.sql.JDBCType;
import java.util.Collections;
import java.util.List;

public class EmbulkPage {
    private final PluginTask task;
    private final PageBuilder pageBuilder;
    private final Schema schema;
    private final Column deleteFlagColumn;
    private final Column fetchedAtColumn;
    private final Column seqColumn;

    public EmbulkPage(PluginTask task, PageBuilder pageBuilder, Schema schema){
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;

        this.deleteFlagColumn = new Column(MysqlBinlogUtil.getDeleteFlagName(task),
                JDBCType.BOOLEAN, "BOOLEAN", Collections.emptyList());
        this.fetchedAtColumn = new Column(MysqlBinlogUtil.getFetchedAtName(task),
                JDBCType.TIMESTAMP, "TIMESTAMP", Collections.emptyList());
        this.seqColumn = new Column(MysqlBinlogUtil.getSeqName(task),
                JDBCType.BIGINT, "BIGINT", Collections.emptyList());
    }

    public void addRecords(List<Row> rows, boolean deleteFlag){
        for (Row row: rows) {
            List<Cell> cells = row.getCells();
            if (task.getEnableMetadataDeleted()){
                Cell deleteFlagCell = new Cell(deleteFlag, deleteFlagColumn);
                cells.add(deleteFlagCell);
            }

            if (task.getEnableMetadataFetchedAt()){
                // value is stored in column visitor
                Cell fetchedAtCell = new Cell(null, fetchedAtColumn);
                cells.add(fetchedAtCell);
            }

            if (task.getEnableMetadataSeq()) {
                Cell seqCell = new Cell(MysqlBinlogUtil.getSeqCounter().incrementAndGet(), seqColumn);
                cells.add(seqCell);
            }

            Row newRow = new Row(cells);

            this.schema.visitColumns(new MysqlBinlogColumnVisitor(new MysqlBinlogAccessor(newRow),
                    this.pageBuilder, this.task));
            this.pageBuilder.addRecord();
        }
    }

    public void flush(){
        this.pageBuilder.flush();
    }
}
