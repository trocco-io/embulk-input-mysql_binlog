package org.embulk.input.mysql_binlog.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.JDBCType;
import java.util.List;

@Data
@NoArgsConstructor
public class Column {
    private String name;
    private JDBCType jdbcType;
    private String typeName;
    private List<String> enumValues;

    public Column(String name, JDBCType jdbcType, String typeName, List<String> enumValues) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.typeName = typeName;
        this.enumValues = enumValues;
    }

    public Column(io.debezium.relational.Column column){
        this(column.name(), JDBCType.valueOf(column.jdbcType()), column.typeName(), column.enumValues());
    }
}
