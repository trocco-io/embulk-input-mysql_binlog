package org.embulk.input.mysql_binlog;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.embulk.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlBinlogInputPlugin
        implements InputPlugin
{
    private final Logger logger = LoggerFactory.getLogger(MysqlBinlogInputPlugin.class);


    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
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
                MysqlBinlogManager binlogManager = new MysqlBinlogManager(task, pageBuilder, schema);
                binlogManager.connect();
            }
        } catch (Exception e) {
            // TODO: handle error
            System.out.println(e.getMessage());
        }

        return Exec.newTaskReport();
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
