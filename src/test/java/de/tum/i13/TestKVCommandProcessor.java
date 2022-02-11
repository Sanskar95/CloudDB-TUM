package de.tum.i13;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static de.tum.i13.server.kv.KVMessage.StatusType.*;
import static org.mockito.Mockito.*;

public class TestKVCommandProcessor {

    @Mock
    KVStoreImpl kvStore;


    @BeforeEach
    void init(){
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void correctParsingOfPut() {

        KVCommandProcessor kvcp = new KVCommandProcessor(kvStore);
        kvcp.process("put key hello 1 2 3");
        when((kvStore.put("key", "hello"))).thenReturn(PUT_SUCCESS + " key");

        verify(kvStore).put("key", "hello 1 2 3");
    }

    @Test
    public void correctParsingOfGet() {

        KVCommandProcessor kvcp = new KVCommandProcessor(kvStore);
        kvcp.process("get key");
        verify(kvStore).get("key");
    }

    @Test
    public void correctParsingOfRemove() {

        KVCommandProcessor kvcp = new KVCommandProcessor(kvStore);
        kvcp.process("delete key");
        when((kvStore.remove("key"))).thenReturn(DELETE_SUCCESS + " key");
        verify(kvStore).remove("key");
    }
}
