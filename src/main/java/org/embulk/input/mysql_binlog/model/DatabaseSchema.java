package org.embulk.input.mysql_binlog.model;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;

public class DatabaseSchema {
    private final MySqlAntlrDdlParser parser;
    private final Tables tables;

    public DatabaseSchema() {
        parser = new MySqlAntlrDdlParser();
        tables = new Tables();
    }

    public void migrate(String sql) {
        parser.parse(sql, tables);
    }

    public io.debezium.relational.Table getTable(String tableName) {
        return tables.forTable(null, null, tableName);
    }
}
