package org.embulk.input.mysql_binlog;

import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;

import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMysqlBinlogUtil {
    private ConfigSource config;
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/mysql_binlog/";

    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName) {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "mysql_binlog", MysqlBinlogInputPlugin.class)
            .build();

    @Test
    public void checkMetadataKey() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals("_trocco_deleted", MysqlBinlogUtil.getDeleteFlagName(task));
        assertEquals("_trocco_fetched_at", MysqlBinlogUtil.getFetchedAtName(task));
    }
}
