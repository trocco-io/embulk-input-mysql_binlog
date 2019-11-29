package org.embulk.input.mysql_binlog;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.input.mysql_binlog.model.MysqlBinlogPosition;
import org.embulk.spi.*;
import org.embulk.spi.type.Types;

public class MysqlBinlogInputPlugin
        implements InputPlugin
{
    private MysqlBinlogManager binlogManager;


    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = buildSchema(task);
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        control.run(taskSource, schema, taskCount);


        // build next config
        ConfigDiff configDiff = Exec.newConfigDiff();
        configDiff.set("from_binlog_filename", MysqlBinlogPosition.getCurrentBinlogFilename());
        configDiff.set("from_binlog_position", MysqlBinlogPosition.getCurrentBinlogPosition());
        configDiff.set("to_binlog_filename", null);
        configDiff.set("to_binlog_position", null);
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try {
            try (PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                this.binlogManager = new MysqlBinlogManager(task, pageBuilder, schema);
                this.binlogManager.connect();
            }
        } catch (Exception e) {
            // TODO: handle error
            System.out.println(e.getMessage());
        }
        return Exec.newTaskReport();
    }

    private Schema buildSchema(PluginTask task){
        int i = 0;
        // TODO: build schema based on sql

        // add meta data
        // todo add metadata based on config
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (ColumnConfig column : task.getColumns().getColumns()) {
            Column outputColumn = new Column(i++, column.getName(), column.getType());
            builder.add(outputColumn);
        }
        // add meta data schema
        Column deleteFlagColumn = new Column(i++, MysqlBinlogUtil.getDeleteFlagName(task), Types.BOOLEAN);
        builder.add(deleteFlagColumn);
        Column fetchedAtColumn = new Column(i++, MysqlBinlogUtil.getFetchedAtName(task), Types.TIMESTAMP);
        builder.add(fetchedAtColumn);

        return new Schema(builder.build());
    }

    @VisibleForTesting
    protected PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }
}
