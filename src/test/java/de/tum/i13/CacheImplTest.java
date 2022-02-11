package de.tum.i13;

import de.tum.i13.server.cache.CacheImpl;
import de.tum.i13.server.exception.KeyNotFoundException;
import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.shared.Constants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class CacheImplTest {
    private static CacheImpl cache;
    private static PrintStream originalOut = System.out;
    private static PrintStream originalErr = System.err;
    private static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    @BeforeAll
    static void before() {

        Path path = Paths.get("data/");
        if (!Files.exists(path)) {
            File dir = new File(path.toString());
            dir.mkdir();
        }
        FileStorage fileStorage = new FileStorage(path);
        cache = new CacheImpl(3, Constants.FIFO, fileStorage);
    }

    @Test
    void putTest() throws NotAllowedException, IOException {
        cache.put("rick", "morty");
        assertEquals(1, cache.currentCacheSize());

    }

    @Test
    void putAndGetTest() throws NotAllowedException, IOException, KeyNotFoundException {
        cache.put("rick", "morty");
        assertEquals("morty", cache.get("rick"));
    }

    @Test
    void putAndDeleteTest() throws NotAllowedException, IOException, KeyNotFoundException {
        cache.put("rick", "morty");
        assertEquals("morty", cache.get("rick"));
        cache.remove("rick");
        assertNull( cache.get("rick"));

    }


    @Test
    void updatePutTest() throws NotAllowedException, IOException, KeyNotFoundException {
        cache.put("rick", "morty");
        cache.put("rick", "mortys");
        assertEquals("mortys", cache.get("rick"));
    }

    @Test
    void testNullKey() throws NotAllowedException, IOException, KeyNotFoundException {
        assertNull(cache.get("some_random_key"));
    }


}
