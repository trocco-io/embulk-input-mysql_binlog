package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import io.debezium.annotation.VisibleForTesting;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.embulk.input.mysql_binlog.manager.TableManager;
import org.embulk.input.mysql_binlog.model.MysqlBinlogPosition;
import org.embulk.input.mysql_binlog.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


// update schema migration in memory
public class QueryEventHandler implements BinlogEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(QueryEventHandler.class);

    private final TableManager tableManager;

    public QueryEventHandler(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public List<String> handle(Event event) {
        QueryEventData queryEvent = event.getData();
        String query = queryEvent.getSql();
        if (!tableManager.getDatabaseName().equals(queryEvent.getDatabase())) {
            return Collections.emptyList();
        }
        if (!shouldProcessQuery(query, tableManager.getTableName())) {
            return Collections.emptyList();
        }

        logger.info("schema migrate");
        logger.info(query);
        logger.debug(normalizeQuery(query, tableManager.getDatabaseName(), tableManager.getTableName()));
        this.tableManager.migrate(normalizeQuery(query, tableManager.getDatabaseName(), tableManager.getTableName()));
        Table table = new Table(tableManager.getDatabaseName(), tableManager.getDatabaseSchema(), tableManager.getTableName(), tableManager.getPluginTask());
        MysqlBinlogPosition.setCurrentDdl(table.toDdl());

        return Collections.emptyList();
    }

    @VisibleForTesting
    public static boolean shouldProcessQuery(String query, String tableName) {
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            if (statement instanceof Alter) {
                Alter alter = (Alter) statement;
                String alterTableName = alter.getTable().getName().replaceAll("`", "");
                return alterTableName.equals(tableName);
            } else {
                return false;
            }
        } catch (Exception e) {
            logger.debug("failback to regex");
            logger.debug(e.getMessage());
            return shouldProcessQueryRegex(query, tableName);
        }
    }

    // First use lib to parse sql
    // if fail use regex to support corner case
    @VisibleForTesting
    public static boolean shouldProcessQueryRegex(String query, String tableName) {
        query = removeSqlComment(query);
        Pattern pattern = Pattern.compile(String.format("^alter\\s+table\\s+.*%s`*\\s+", tableName), Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(query.trim());
        return matcher.find();
    }

    @VisibleForTesting
    public static String removeSqlComment(String query) {
        return query.replaceAll("(--.*)|(((/\\*)+?[\\w\\W]+?(\\*/)+))", "");
    }

    @VisibleForTesting
    public static String normalizeQuery(String query, String databaseName, String tableName) {
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            if (statement instanceof Alter) {
                Alter alter = (Alter) statement;
                StringBuilder sb = new StringBuilder();
                sb.append("ALTER TABLE ");
                sb.append("`");
                sb.append(tableName);
                sb.append("` ");
                String alterExpressions = alter.getAlterExpressions().stream().map(AlterExpression::toString).collect(Collectors.joining(","));
                sb.append(alterExpressions);
                return sb.toString();
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.debug("fail back to regex");
            logger.debug(e.getMessage());
            return normalizeQueryRegex(query, databaseName, tableName);
        }
    }

    // ALTER TABLE `cdc`.`sample` ADD COLUMN `add2` float NULL COMMENT ''
    // ALTER TABLE cdc.`sample` ADD COLUMN `add2` float NULL COMMENT ''
    // ALTER TABLE `cdc`.sample ADD COLUMN `add2` float NULL COMMENT ''
    // ALTER TABLE cdc.sample ADD COLUMN `add2` float NULL COMMENT ''
    // => ALTER TABLE `sample` ADD COLUMN `add2` float NULL COMMENT ''
    @VisibleForTesting
    public static String normalizeQueryRegex(String query, String databaseName, String tableName) {
        // TODO: use antlr or other library
        // JSQLParser does not work for this case
        // "ALTER TABLE `mytable` CHANGE `enum_col` `enum_col` enum(\"foo\", \"bar\") CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT ''";
        query = removeSqlComment(query);
        String regex = String.format("^alter\\s+table\\s+`*%s`*\\s*\\.\\s*`*%s`*", databaseName, tableName);
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).replaceFirst(String.format("ALTER TABLE `%s`", tableName));
    }
}
