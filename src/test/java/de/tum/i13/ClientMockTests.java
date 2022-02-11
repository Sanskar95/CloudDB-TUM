package de.tum.i13;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.Milestone1Main;
import de.tum.i13.shared.Constants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;

import java.io.IOException;

import static de.tum.i13.server.kv.KVMessage.StatusType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Tests for the client implementation using a mock connection
 */
public class ClientMockTests {

    @Mock
    ActiveConnection activeConnectionMock;

    @Captor
    ArgumentCaptor<String> argumentCaptor;

    @BeforeEach
    void init(){
        MockitoAnnotations.initMocks(this);
    }
    @Test
    public void put(){
        assertNotNull(activeConnectionMock);
        assertNotNull(argumentCaptor);

        Milestone1Main.activeConnection = activeConnectionMock;
        try {
            when((activeConnectionMock.readline())).thenReturn(PUT_SUCCESS + " key1 value1");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Milestone1Main.putKey(new String[]{"put","key1", "value1"}, "put key1 value1");
        verify(activeConnectionMock).write(argumentCaptor.capture());
        String message = argumentCaptor.getValue();
        assertEquals(message, "put key1 value1");
    }

    @Test
    public void get(){
        assertNotNull(activeConnectionMock);
        assertNotNull(argumentCaptor);
        Milestone1Main.activeConnection = activeConnectionMock;
        try {
            when((activeConnectionMock.readline())).thenReturn(GET_SUCCESS + " key1 value1");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Milestone1Main.getKey(new String[]{Constants.GET,"key1"}, "get key1");
        verify(activeConnectionMock).write(argumentCaptor.capture());
        String message = argumentCaptor.getValue();
        assertEquals(message, "get key1");
    }

    @Test
    public void delete(){
        assertNotNull(activeConnectionMock);
        assertNotNull(argumentCaptor);
        Milestone1Main.activeConnection = activeConnectionMock;

        try {
            when((activeConnectionMock.readline())).thenReturn(DELETE_SUCCESS + " key1");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Milestone1Main.putKey(new String[]{"put","key1"}, "put key1");
        verify(activeConnectionMock).write(argumentCaptor.capture());
        String message = argumentCaptor.getValue();
        assertEquals(message, "delete key1");
    }
}
