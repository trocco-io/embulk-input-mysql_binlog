package org.embulk.input.mysql_binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.embulk.input.mysql_binlog.handler.BinlogEventHandler;
import org.embulk.input.mysql_binlog.handler.PositionHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class TestMysqlBinlogEventHandler {
    private MysqlBinlogEventHandler handler;
    private BinlogEventHandler mockHandler;
    private PositionHandler mockPositionHandler;
    private Event mockEvent;
    private Event mockEventWriteRows;
    private EventHeaderV4 mockHeaderWriteRows;
    private EventType eventType;
    private EventType mockEventTypeWriteRows;

    @Before
    public void setup() {
        handler = new MysqlBinlogEventHandler();
        mockHandler = Mockito.mock(BinlogEventHandler.class);
        mockPositionHandler = Mockito.mock(PositionHandler.class);
        mockEventWriteRows = Mockito.mock(Event.class);
        mockHeaderWriteRows = Mockito.mock(EventHeaderV4.class);

        mockEventTypeWriteRows = EventType.WRITE_ROWS;

        when(mockEventWriteRows.getHeader()).thenReturn(mockHeaderWriteRows);
        when(mockHeaderWriteRows.getEventType()).thenReturn(mockEventTypeWriteRows);
    }

    @Test
    public void testRegisterHandler() {
        handler.registerPositionHandler(mockPositionHandler);
        when(mockPositionHandler.shouldHandle(mockEventWriteRows)).thenReturn(true);
        eventType = mockEventTypeWriteRows;
        mockEvent = mockEventWriteRows;

        handler.registerHandler(mockHandler, eventType);
        handler.handle(mockEvent);

        verify(mockHandler, times(1)).handle(mockEvent);
    }

    @Test
    public void testRegisterHandler__NotCalled() {
        handler.registerPositionHandler(mockPositionHandler);
        when(mockPositionHandler.shouldHandle(mockEventWriteRows)).thenReturn(true);
        eventType = EventType.QUERY;
        mockEvent = mockEventWriteRows;

        handler.registerHandler(mockHandler, eventType);
        handler.handle(mockEvent);

        verify(mockHandler, times(0)).handle(mockEvent);
    }

    @Test
    public void testRegisterAlwaysHandler() {
        handler.registerPositionHandler(mockPositionHandler);
        when(mockPositionHandler.shouldHandle(mockEventWriteRows)).thenReturn(true);
        eventType = mockEventTypeWriteRows;
        mockEvent = mockEventWriteRows;

        handler.registerAlwaysHandler(mockHandler, eventType);
        handler.handle(mockEvent);

        verify(mockHandler, times(1)).handle(mockEvent);
    }

    @Test
    public void testRegisterAlwaysHandler__NotCalled() {
        handler.registerPositionHandler(mockPositionHandler);
        when(mockPositionHandler.shouldHandle(mockEventWriteRows)).thenReturn(true);
        eventType = EventType.QUERY;
        mockEvent = mockEventWriteRows;

        handler.registerAlwaysHandler(mockHandler, eventType);
        handler.handle(mockEvent);

        verify(mockHandler, times(0)).handle(mockEvent);
    }

    @Test
    public void testRegisterPositionHandler() {
        handler.registerPositionHandler(mockPositionHandler);
        when(mockPositionHandler.shouldHandle(mockEventWriteRows)).thenReturn(true);
        eventType = mockEventTypeWriteRows;
        mockEvent = mockEventWriteRows;

        handler.registerHandler(mockHandler, eventType);
        handler.handle(mockEvent);

        verify(mockPositionHandler, times(1)).handle(mockEvent);
    }

    @Test
    public void testShouldHandleEvent() {
        handler.registerPositionHandler(mockPositionHandler);
        when(mockPositionHandler.shouldHandle(mockEventWriteRows)).thenReturn(true);
        eventType = mockEventTypeWriteRows;
        mockEvent = mockEventWriteRows;

        handler.registerHandler(mockHandler, eventType);
        handler.handle(mockEvent);

        assert handler.shouldHandleEvent(mockEvent);
    }
}
