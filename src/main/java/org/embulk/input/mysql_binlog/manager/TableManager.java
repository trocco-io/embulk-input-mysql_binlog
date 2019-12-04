package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import lombok.Getter;
import org.embulk.input.mysql_binlog.MysqlBinlogInputPlugin;
import org.embulk.input.mysql_binlog.model.Column;
import org.embulk.input.mysql_binlog.model.DbInfo;
import org.embulk.input.mysql_binlog.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableManager {
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("jdbc client not found");
        }
    }
    private final Logger logger = LoggerFactory.getLogger(TableManager.class);
    private Map<Long, Table> tableInfo;
    private DbInfo dbInfo;

    @Getter
    private String targetTableName;

    public TableManager(DbInfo dbInfo, String targetTableName){
        this.dbInfo = dbInfo;
        this.tableInfo = new HashMap<>();
        this.targetTableName = targetTableName;
    }

    public Table getTableInfo(Long tableId) {
        if (tableInfo.containsKey(tableId)){
            return tableInfo.get(tableId);
        }
        throw new RuntimeException("tableId does not exists");
    }

    public void setTableInfo(TableMapEventData eventData){
        String tableName = eventData.getTable();
        String dbName = eventData.getDatabase();
        long tableId =  eventData.getTableId();

        if (tableInfo.containsKey(eventData.getTableId())){
            return;
        }
        String url = "jdbc:mysql://" + dbInfo.getHost() + ":" + dbInfo.getPort() + "/" + dbName + "?characterEncoding=UTF-8&autoReconnect=true";

        try (Connection con = DriverManager.getConnection(url, dbInfo.getUser(), dbInfo.getPassword())) {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet dbColumns = metaData.getColumns(dbName, null, tableName, null);
            Table table = new Table(dbName, tableName);

            List<ColumnType> columnTypes = new ArrayList<>();
            for (byte columnType: eventData.getColumnTypes()) {
                columnTypes.add(ColumnType.byCode(columnType));
            }
            List<Column> columns = new ArrayList<>();
            int i = 0;
            while (dbColumns.next()) {
                String columnName = dbColumns.getString("COLUMN_NAME");
                // &0xFF unsigned byte to int
                System.out.println(columnName);
                System.out.println(eventData.getColumnTypes()[i]& 0xFF);
                columns.add(new Column(columnName,
                        ColumnType.byCode(eventData.getColumnTypes()[i]& 0xFF),
                        JDBCType.valueOf(dbColumns.getInt("DATA_TYPE"))));
                i++;
            }
            table.setColumns(columns);
            tableInfo.put(tableId, table);
            System.out.println(table);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Table buildTable(DbInfo dbInfo, String targetTableName){
        Table table = new Table(dbInfo.getDbName(), targetTableName);
        List<Column> columns = new ArrayList<>();
        DatabaseMetaData metaData = fetchDatabaseMetaData(dbInfo);
        try {
            ResultSet dbColumns = metaData.getColumns(dbInfo.getDbName(), null, targetTableName, null);
            int i = 0;
            while (dbColumns.next()) {
                String columnName = dbColumns.getString("COLUMN_NAME");
                columns.add(new Column(columnName,
                        null,
                        JDBCType.valueOf(dbColumns.getInt("DATA_TYPE"))));
                i++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        table.setColumns(columns);
        return table;
    }

    public static DatabaseMetaData fetchDatabaseMetaData(DbInfo dbInfo){
        String url = "jdbc:mysql://" + dbInfo.getHost() + ":" + dbInfo.getPort() + "/" + dbInfo.getDbName() + "?characterEncoding=UTF-8&autoReconnect=true";
        try (Connection con = DriverManager.getConnection(url, dbInfo.getUser(), dbInfo.getPassword())) {
            return con.getMetaData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
