package org.embulk.input.mysql_binlog.model;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.JDBCType;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Column {
    private String name;
    private ColumnType columnType;
    private JDBCType jdbcType;
}
