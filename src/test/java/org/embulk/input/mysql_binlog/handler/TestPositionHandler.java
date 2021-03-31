package org.embulk.input.mysql_binlog.handler;

import com.github.shyiko.mysql.binlog.event.*;
import org.embulk.input.mysql_binlog.PluginTask;
import org.embulk.input.mysql_binlog.manager.MysqlBinlogManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;

import static org.mockito.Mockito.*;


public class TestPositionHandler {
    private Event rotateEvent;
    private Event insertEvent;
    private MysqlBinlogManager binlogManager;
    @Spy
    private PositionHandler positionHandler;
    private PluginTask task;

    @Before
    public void prepare(){
        binlogManager = mock(MysqlBinlogManager.class);
        positionHandler = spy(new PositionHandler(binlogManager));
        doReturn(false).when(positionHandler).isFinish(any(), anyString(), anyLong());
    }

    @Before
    public void prepareRotateEvent(){
        rotateEvent = mock(Event.class);
        EventHeaderV4 header = mock(EventHeaderV4.class);
        RotateEventData data = mock(RotateEventData.class);
        doReturn(1234L).when(header).getNextPosition();
        doReturn(EventType.ROTATE).when(header).getEventType();
        doReturn(header).when(rotateEvent).getHeader();
        doReturn("mysql-bin.000002").when(data).getBinlogFilename();
        doReturn(data).when(rotateEvent).getData();
    }

    @Before
    public void prepareInsertEvent(){
        insertEvent = mock(Event.class);
        EventHeaderV4 header = mock(EventHeaderV4.class);
        WriteRowsEventData data = mock(WriteRowsEventData.class);
        doReturn(5678L).when(header).getNextPosition();
        doReturn(EventType.WRITE_ROWS).when(header).getEventType();
        doReturn(header).when(insertEvent).getHeader();
        doReturn(data).when(insertEvent).getData();
    }

    @Test
    public void storeBinlogFilenameAndPosition() {
        positionHandler.handle(rotateEvent);
        verify(binlogManager).setBinlogFilename("mysql-bin.000002");
        verify(binlogManager).setBinlogPosition(1234L);
    }

    @Test
    public void storeOnlyPosition(){
        positionHandler.handle(insertEvent);
        verify(binlogManager, never()).setBinlogFilename(anyString());
        verify(binlogManager).setBinlogPosition(5678L);
    }
}
