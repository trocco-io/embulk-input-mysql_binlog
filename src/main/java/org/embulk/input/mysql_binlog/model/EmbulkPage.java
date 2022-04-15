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
import java.util.Arrays;

public class EmbulkPage {
    private final PluginTask task;
    private final PageBuilder pageBuilder;
    private final Schema schema;
    private final Column deleteFlagColumn;
    private final Column fetchedAtColumn;
    private final Column seqColumn;
    private final Column databaseColumn;
    private final Column tableColumn;
    private final Column dataColumn;

    public EmbulkPage(PluginTask task, PageBuilder pageBuilder, Schema schema){
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;

        this.deleteFlagColumn = new Column(MysqlBinlogUtil.getDeleteFlagName(task),
                JDBCType.BOOLEAN, "BOOLEAN", Collections.emptyList(), task);
        this.fetchedAtColumn = new Column(MysqlBinlogUtil.getFetchedAtName(task),
                JDBCType.TIMESTAMP, "TIMESTAMP", Collections.emptyList(), task);
        this.seqColumn = new Column(MysqlBinlogUtil.getSeqName(task),
                JDBCType.BIGINT, "BIGINT", Collections.emptyList(), task);

        this.dataColumn = new Column("data",
                JDBCType.VARCHAR, "STRING", Collections.emptyList(), task);
        this.databaseColumn = new Column("database",
                JDBCType.VARCHAR, "STRING", Collections.emptyList(), task);
        this.tableColumn = new Column("table",
                JDBCType.VARCHAR, "STRING", Collections.emptyList(), task);
    }

    public void addRecords(List<Row> rows, boolean deleteFlag){
        for (Row row: rows) {
            List<Cell> cells;
            if (this.task.getDataAsJson()){
                String data = row.toJsonString();
                Table table = row.getTable();

                Cell databaseCell = new Cell(table.getDatabaseName(), databaseColumn);
                Cell tableCell = new Cell(table.getTableName(), tableColumn);
                Cell dataCell = new Cell(data, dataColumn);
                cells = Arrays.asList(
                    databaseCell,
                    tableCell,
                    dataCell);
            }else{
                cells = row.getCells();
            }

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

            Row newRow = new Row(cells, row.getTable());

            this.schema.visitColumns(new MysqlBinlogColumnVisitor(new MysqlBinlogAccessor(newRow),
                    this.pageBuilder, this.task));
            this.pageBuilder.addRecord();
        }
    }

    public void flush(){
        this.pageBuilder.flush();
    }
}
