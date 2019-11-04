package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import lombok.Getter;
import org.embulk.input.mysql_binlog.model.Column;
import org.embulk.input.mysql_binlog.model.DbInfo;
import org.embulk.input.mysql_binlog.model.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableManager {
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
        String talbeName = eventData.getTable();
        String dbName = eventData.getDatabase();
        long tableId =  eventData.getTableId();

        if (tableInfo.containsKey(eventData.getTableId())){
            return;
        }
        String url = "jdbc:mysql://" + dbInfo.getHost() + ":" + dbInfo.getPort() + "/" + dbName + "?characterEncoding=UTF-8&autoReconnect=true";

        try (Connection con = DriverManager.getConnection(url, dbInfo.getUser(), dbInfo.getPassword())) {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet dbColumns = metaData.getColumns(dbName, null, talbeName, null);
            Table table = new Table(dbName, talbeName);

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
}
