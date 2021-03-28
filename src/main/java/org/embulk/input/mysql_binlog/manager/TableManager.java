package org.embulk.input.mysql_binlog.manager;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import lombok.Getter;
import org.embulk.input.mysql_binlog.PluginTask;
import org.embulk.input.mysql_binlog.model.DatabaseSchema;
import org.embulk.input.mysql_binlog.model.DbInfo;
import org.embulk.input.mysql_binlog.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TableManager {
    private final Logger logger = LoggerFactory.getLogger(TableManager.class);
    private Map<Long, Table> tableInfo;
    private DbInfo dbInfo;
    private PluginTask pluginTask;

    @Getter
    private DatabaseSchema databaseSchema;

    public TableManager(DbInfo dbInfo, PluginTask pluginTask){
        this.dbInfo = dbInfo;
        this.tableInfo = new HashMap<>();
        this.pluginTask = pluginTask;
        this.databaseSchema = new DatabaseSchema();
    }

    public String getDatabaseName(){
        return pluginTask.getDatabase();
    }

    public String getTableName(){
        return pluginTask.getTable();
    }

    public Table getTableInfo(Long tableId) {
        if (tableInfo.containsKey(tableId)){
            return tableInfo.get(tableId);
        }
        return null;
    }

    public void migrate(String sql){
        this.databaseSchema.migrate(sql);
    }

    public void setTableInfo(TableMapEventData eventData){
        String tableName = eventData.getTable();
        String dbName = eventData.getDatabase();
        long tableId =  eventData.getTableId();
        Table table = new Table(dbName, databaseSchema, tableName);
        tableInfo.put(tableId, table);
    }
}
