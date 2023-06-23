package org.embulk.input.mysql_binlog.handler;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestQueryEventHandler {
    @Test
    public void shouldProcessQuery__true() {
        String query = "ALTER TABLE `cdc`.`sample` ADD COLUMN `add2` float NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertTrue(result);
    }

    @Test
    public void shouldProcessQuery_space__true() {
        String query = "ALTER TABLE `cdc` . `sample` ADD COLUMN `add2` float NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertTrue(result);
    }

    @Test
    public void shouldProcessQuery_not_quote__true() {
        String query = "ALTER TABLE cdc.`sample` ADD COLUMN `add2` float NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertTrue(result);
    }

    @Test
    public void shouldProcessQuery_character_failback__true() {
        String query = "ALTER TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertTrue(result);
    }

    @Test
    public void shouldProcessQuery_character_failback_with_db__true() {
        String query = "ALTER TABLE cdc.`sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertTrue(result);
    }

    @Test
    public void shouldProcessQuery_character_failback_with_db__false() {
        String query = "ALTER TABLE cdc.`false` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertFalse(result);
    }

    @Test
    public void shouldProcessQuery_character_failback__false() {
        String query = "ALTER TABLE `mytable` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertFalse(result);
    }

    @Test
    public void shouldProcessQuery__false() {
        String query = "ALTER TABLE `cdc`.`sample2` ADD COLUMN `add2` float NULL COMMENT ''";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertFalse(result);
    }

    @Test
    public void shouldProcessQuery_parse_error_false() {
        String query = "BEGIN";
        String tableName = "sample";
        boolean result = QueryEventHandler.shouldProcessQuery(query, tableName);
        assertFalse(result);
    }

    @Test
    public void normalizeQuery() {
        String query = "ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__add_quote() {
        String query = "ALTER TABLE sample ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__trim_db() {
        String query = "ALTER TABLE `db`.`sample2` ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__no_quote_db() {
        String query = "ALTER TABLE db.`sample` ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__no_quote_table() {
        String query = "ALTER TABLE `db`.sample ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__trim_comment() {
        String query = "ALTER /* comment */ TABLE `db`.sample ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__trim_comment2() {
        String query = "ALTER/*comment*/TABLE `db`.sample ADD COLUMN `add2` float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__trim_comment3() {
        String query = "ALTER  TABLE `db`.sample ADD COLUMN `add2` /* comment */ float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery__trim_comment4() {
        String query = "ALTER -- comment\n"
                + "TABLE `db`.sample ADD COLUMN `add2` /* comment */ float NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery_character_failback() {
        String query = "ALTER TABLE db.`sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery_character_failback_no_db() {
        String query = "ALTER TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        assertEquals("ALTER TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery_character_failback_comment() {
        String query = "ALTER /* comment */ TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        // TODO: remove space
        assertEquals("ALTER  TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''", result);
    }

    @Test
    public void normalizeQuery_character_failback_comment2() {
        String query = "ALTER -- comment\n"
                + "TABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        String databaseName = "db";
        String tableName = "sample";
        String result = QueryEventHandler.normalizeQuery(query, databaseName, tableName);
        // TODO: remove break line
        assertEquals("ALTER \nTABLE `sample` CHANGE `enum_col` `enum_col` enum(\\\"foo\\\", \\\"bar\\\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''", result);
    }
}
