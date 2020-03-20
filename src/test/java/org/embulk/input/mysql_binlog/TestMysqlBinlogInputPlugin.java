package org.embulk.input.mysql_binlog;

import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMysqlBinlogInputPlugin
{
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
    public void checkBasicConfig() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals("localhost", task.getHost());
        assertEquals(3306, task.getPort());
        assertEquals("test", task.getDatabase());
        assertEquals("test", task.getTable());
        assertEquals("root", task.getUser());
        assertEquals("root", task.getPassword());
        assertEquals("mysql-bin.000001", task.getFromBinlogFilename());
        assertEquals(Long.valueOf(4), task.getFromBinlogPosition());
        assertTrue(task.getEnableMetadata());
        assertEquals("_trocco_", task.getMetadataPrefix());
        assertFalse(task.getToBinlogFilename().isPresent());
        assertFalse(task.getToBinlogPosition().isPresent());
        assertNotNull(task.getColumns());
        assertEquals("UTC", task.getDefaultTimezone());
    }
}
