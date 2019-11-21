package org.embulk.input.mysql_binlog.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class Table {
    private String dbName;
    private String tableName;
    private List<Column> columns;

    public Table(String dbName, String tableName){
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public List<Row> convertRows(List<Serializable[]> rawRows){
        List<Row> rows = new ArrayList<>();

        for (Serializable[] rawRow:
                rawRows) {
            Row row = convertRow(rawRow);
            rows.add(row);
        }
        return rows;
    }

    private Row convertRow(Serializable[] rawRow){
        List<Cell> cells = new ArrayList<>();
        Row row = new Row(cells);
        for (int i = 0; i < rawRow.length; i++){
            Column column = columns.get(i);
            cells.add(new Cell(rawRow[i], column));
        }
        return row;
    }
}
