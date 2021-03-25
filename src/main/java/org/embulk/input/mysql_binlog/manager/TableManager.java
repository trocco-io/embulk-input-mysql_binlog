package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import lombok.Getter;
import org.embulk.input.mysql_binlog.PluginTask;
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
import java.util.Properties;

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
    private PluginTask pluginTask;

    @Getter
    private String targetTableName;

    public TableManager(DbInfo dbInfo, PluginTask pluginTask){
        this.dbInfo = dbInfo;
        this.tableInfo = new HashMap<>();
        this.pluginTask = pluginTask;
        this.targetTableName = pluginTask.getTable();
    }

    public Table getTableInfo(Long tableId) {
        if (tableInfo.containsKey(tableId)){
            return tableInfo.get(tableId);
        }
        throw new RuntimeException("tableId does not exist");
    }

    public void setTableInfo(TableMapEventData eventData){
        System.out.println("setTableInfo 1");
        String talbeName = eventData.getTable();
        String dbName = eventData.getDatabase();
        long tableId =  eventData.getTableId();

        if (tableInfo.containsKey(eventData.getTableId())){
            return;
        }
        String url = String.format("jdbc:mysql://%s:%d/%s",
                dbInfo.getHost(), dbInfo.getPort(), dbInfo.getDbName());

        Properties props = new Properties();
        props.setProperty("user", dbInfo.getUser());
        props.setProperty("password", dbInfo.getPassword());
        props.setProperty("characterEncoding", "UTF-8");
        props.setProperty("autoReconnect", "true");
        System.out.println("setTableInfo 2");

        switch (pluginTask.getSsl()) {
            case DISABLE:
                props.setProperty("useSSL", "false");
                break;
            case ENABLE:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "false");
                break;
            case VERIFY:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "true");
                break;
        }
        System.out.println("setTableInfo 3");

        try (Connection con = DriverManager.getConnection(url, props)) {
            System.out.println("setTableInfo 4");
            DatabaseMetaData metaData = con.getMetaData();
            System.out.println("setTableInfo 5");
            ResultSet dbColumns = metaData.getColumns(dbName, null, talbeName, null);
            System.out.println("setTableInfo 6");
            Table table = new Table(dbName, talbeName);
            System.out.println("setTableInfo 7");

            List<ColumnType> columnTypes = new ArrayList<>();
            for (byte columnType: eventData.getColumnTypes()) {
                columnTypes.add(ColumnType.byCode(columnType));
            }
            System.out.println("setTableInfo 8");
            List<Column> columns = new ArrayList<>();
            int i = 0;
            while (dbColumns.next()) {

                String columnName = dbColumns.getString("COLUMN_NAME");
                System.out.println(columnName);
                // &0xFF unsigned byte to int
                columns.add(new Column(columnName,
                        ColumnType.byCode(eventData.getColumnTypes()[i]& 0xFF),
                        JDBCType.valueOf(dbColumns.getInt("DATA_TYPE"))));
                i++;

            }
            System.out.println("setTableInfo 9");
            table.setColumns(columns);
            System.out.println("setTableInfo 10");
            tableInfo.put(tableId, table);
            System.out.println("setTableInfo 11");
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
    }
}
