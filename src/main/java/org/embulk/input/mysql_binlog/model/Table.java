package org.embulk.input.mysql_binlog.model;

import lombok.Data;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class Table {
    private String databaseName;
    @Setter
    private DatabaseSchema databaseSchema;
    private String tableName;

    public Table(String dbName, DatabaseSchema databaseSchema, String tableName){
        this.databaseName = dbName;
        this.databaseSchema = databaseSchema;
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
        io.debezium.relational.Table table = this.databaseSchema.getTable(tableName);
        List<io.debezium.relational.Column> columns = table.columns();
        Row row = new Row(cells);
        for (int i = 0; i < rawRow.length; i++){
            io.debezium.relational.Column column = columns.get(i);
            cells.add(new Cell(rawRow[i], new Column(column)));
        }
        return row;
    }

    public String toDdl(){
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        String lines = databaseSchema.getTable(tableName).columns().stream()
                .map(this::ddlLine)
                .collect(Collectors.joining(","));
        sb.append(lines);
        sb.append(");");
        return sb.toString();
    }

    private String ddlLine(io.debezium.relational.Column column){
        StringBuilder sb = new StringBuilder();
        sb.append(column.name());
        sb.append(" ");
        sb.append(column.typeName());

        //e.g. enum_col ENUM ('foo', 'bar')
        if (column.typeName().equals("ENUM")){
            sb.append("(");
            String enumVals = column.enumValues().stream()
                    .map(v->v.substring(1, v.length()-1))
                    .map(v->String.format("'%s'", v))
                    .collect(Collectors.joining(","));
            sb.append(enumVals);
            sb.append(")");
        }else{
            // e.g. varchar(255)
            if (column.length() > 0){
                sb.append("(");
                sb.append(column.length());
                sb.append(")");
            }
        }
        return sb.toString();
    }
}
