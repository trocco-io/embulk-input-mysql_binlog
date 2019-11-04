package org.embulk.input.mysql_binlog;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.SchemaConfig;

public interface PluginTask
        extends Task {
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("3306")
    int getPort();

    @Config("database")
    String getDatabase();

    @Config("table")
    String getTable();


    @Config("user")
    String getUser();

    @Config("password")
    String getPassword();

    @Config("binlog_filename")
    String getBinlogFilename();

    @Config("binlog_position")
    Long getBinlogPosition();

    @Config("enable_metadata")
    @ConfigDefault("true")
    boolean getEnableMetadata();

    @Config("metadata_prefix")
    @ConfigDefault("\"_\"")
    String getMetadataPrefix();

    @Config("columns")
    SchemaConfig getColumns();

}
