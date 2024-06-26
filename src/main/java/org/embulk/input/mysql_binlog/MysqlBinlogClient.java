package org.embulk.input.mysql_binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.embulk.input.mysql_binlog.handler.*;
import org.embulk.input.mysql_binlog.model.DbInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MysqlBinlogClient implements BinaryLogClient.LifecycleListener {
    private final Logger logger = LoggerFactory.getLogger(MysqlBinlogClient.class);
    private final BinaryLogClient client;
    private boolean isConnecting = true;

    private boolean isError = false;

    public boolean getConnecting() {
        return isConnecting;
    }

    public void setConnecting(boolean connecting) {
        isConnecting = connecting;
    }


    public MysqlBinlogClient(DbInfo dbInfo, String binlogFilename, SSLMode sslMode) {
        client = new BinaryLogClient(dbInfo.getHost(), dbInfo.getPort(), dbInfo.getUser(), dbInfo.getPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );
        client.setEventDeserializer(eventDeserializer);
        client.setBinlogFilename(binlogFilename);
        client.setBlocking(false);
        client.registerLifecycleListener(this);
        client.setHeartbeatInterval(client.getKeepAliveInterval() / 2);
        client.setSSLMode(sslMode);
    }

    public void registerEventListener(BinlogEventHandler binlogEventHandler) {
        client.registerEventListener(event -> {
            logger.debug(event.toString());
            if (isConnecting) {
                // TODO: add filter
                // TODO: pass client and handle binlog position and disconnect
                binlogEventHandler.handle(event);
            }
        });
    }

    public void connect() throws IOException {
        client.connect();
    }

    public void disconnect() throws IOException {
        client.disconnect();
    }


    public static DbInfo convertTaskToDbInfo(PluginTask task) {
        return new DbInfo(task.getHost(), task.getPort(), task.getDatabase(), task.getUser(), task.getPassword());
    }

    @Override
    public void onConnect(BinaryLogClient client) {
        logger.info("connect");
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
        logger.warn("communication failure", ex);
        synchronized (this){
           isError = true;
        }
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
        logger.warn("event deserialization failure", ex);
        synchronized (this){
            isError = true;
        }
    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
        logger.info("disconnect");
        synchronized (this) {
            if (isError) {
                throw new RuntimeException("Error was occurred");
            }
        }
    }
}
