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

    @Config("from_binlog_filename")
    String getFromBinlogFilename();

    @Config("from_binlog_position")
    Long getFromBinlogPosition();

    @Config("to_binlog_filename")
    @ConfigDefault("null")
    Optional<String> getToBinlogFilename();

    @Config("to_binlog_position")
    @ConfigDefault("null")
    Optional<Long> getToBinlogPosition();

    @Config("enable_metadata_delete_flag")
    @ConfigDefault("true")
    boolean getEnableMetadataDeleteFlag();

    @Config("enable_metadata_fetched_at")
    @ConfigDefault("true")
    boolean getEnableMetadataFetchedAt();

    @Config("metadata_prefix")
    @ConfigDefault("\"_\"")
    String getMetadataPrefix();

    @Config("columns")
    SchemaConfig getColumns();

}
