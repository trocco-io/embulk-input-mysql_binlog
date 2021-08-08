package org.embulk.input.mysql_binlog.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.embulk.input.mysql_binlog.PluginTask;

import java.sql.JDBCType;
import java.util.List;

@Data
@NoArgsConstructor
public class Column {
    private String name;
    private JDBCType jdbcType;
    private String typeName;
    private List<String> enumValues;
    private String timezone;
    private PluginTask task;

    public Column(String name, JDBCType jdbcType, String typeName, List<String> enumValues, PluginTask task) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.typeName = typeName;
        this.enumValues = enumValues;
        this.task = task;
    }

    public Column(io.debezium.relational.Column column, PluginTask task){
        this(column.name(), JDBCType.valueOf(column.jdbcType()), column.typeName(), column.enumValues(), task);
    }

    public PluginTask getTask() {
        return task;
    }
}
