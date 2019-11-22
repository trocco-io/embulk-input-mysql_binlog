package org.embulk.input.mysql_binlog;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.spi.*;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlBinlogInputPlugin
        implements InputPlugin
{
    private MysqlBinlogManager binlogManager;
    private final Logger logger = LoggerFactory.getLogger(MysqlBinlogInputPlugin.class);


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
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
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
                binlogManager = new MysqlBinlogManager(task, pageBuilder, schema);
                binlogManager.connect();
            }
        } catch (Exception e) {
            // TODO: handle error
            System.out.println(e.getMessage());
        }
        System.out.println(binlogManager.getBinlogFilename());
        System.out.println(binlogManager.getBinlogPosition());
        return Exec.newTaskReport();
    }

    private Schema buildSchema(PluginTask task){
        int i = 0;
        // add meta data
        // todo add metadata based on config
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (ColumnConfig column : task.getColumns().getColumns()) {
            Column outputColumn = new Column(i++, column.getName(), column.getType());
            builder.add(outputColumn);
        }
        Column deleteFlagColumn = new Column(i++, MysqlBinlogUtil.getUpdateAtColumnName(task), Types.BOOLEAN);
        builder.add(deleteFlagColumn);
        Column updatedAtColumn = new Column(i++, MysqlBinlogUtil.getUpdateAtColumnName(task), Types.TIMESTAMP);
        builder.add(updatedAtColumn);

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
