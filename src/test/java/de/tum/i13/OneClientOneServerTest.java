package de.tum.i13;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.Milestone1Main;
import de.tum.i13.ecs.SimpleEcsServer;
import de.tum.i13.ecs.StartEcsServer;
import de.tum.i13.ecs.ecsprocessor.EcsCmdProcessor;
import de.tum.i13.ecs.ecsprocessor.EcsImplementation;
import de.tum.i13.server.kv.EcsCommandProcessor;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStoreImpl;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests one client doing requests on a single server
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OneClientOneServerTest {

    private static Thread NioThread;
    private static Thread EcsThread;

    private static final int NIO_PORT = 5152;
    private static final int ECS_PORT = 5500;
    private static final String HOST = "127.0.0.1";

    private static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    private static void launchServer(boolean test) throws IOException, InterruptedException {
        Path path = Paths.get("data/");
        if (!Files.exists(path)) {
            File dir = new File(path.toString());
            dir.mkdir();
        }


        //
        NioThread = new Thread() {
            @Override
            public void run() {
                try {
                    StartSimpleNioServer.main(new String[]{"-p",NIO_PORT+"","-d","data/","-b",HOST+":"+ECS_PORT});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        EcsThread = new Thread() {
            @Override
            public void run() {
                try {
                    StartEcsServer.main(new String[]{"-p",ECS_PORT+""});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        EcsThread.start();
        Thread.sleep(1000);
        NioThread.start();
        Thread.sleep(2000);
        Milestone1Main.metaData = new MetaData();
    }

    @BeforeAll
    public static void launchServer() throws IOException, InterruptedException {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        launchServer(true);
    }


    @Test
    @Order(1)
    public void put(){
        ActiveConnection activeConnection = Milestone1Main.buildconnection(new String[]{"connect",HOST, ""+NIO_PORT});
        System.out.println("Here");
        assertNotNull(activeConnection);

        Milestone1Main.activeConnection = activeConnection;
        System.out.println(outContent.toString());
        outContent.reset();
        Milestone1Main.putKey(new String[]{"put","key1", "value1"}, "put key1 value1");
        assertEquals("EchoClient> " + KVMessage.StatusType.PUT_SUCCESS.toString().toLowerCase(Locale.ROOT) + " key1", outContent.toString().replace("\r\n",""));


        try {
            activeConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(2)
    public void get(){
        ActiveConnection activeConnection = Milestone1Main.buildconnection(new String[]{"connect",HOST, ""+NIO_PORT});
        assertNotNull(activeConnection);
        Milestone1Main.activeConnection = activeConnection;

        outContent.reset();
        Milestone1Main.getKey(new String[]{Constants.GET,"key1"}, "get key1");
        assertEquals("EchoClient> " + KVMessage.StatusType.GET_SUCCESS.toString().toLowerCase(Locale.ROOT) + " key1 value1", outContent.toString().replace("\r\n",""));

        try {
            activeConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(3)
    public void delete(){
        ActiveConnection activeConnection = Milestone1Main.buildconnection(new String[]{"connect",HOST, ""+NIO_PORT});
        assertNotNull(activeConnection);
        Milestone1Main.activeConnection = activeConnection;

        outContent.reset();
        Milestone1Main.putKey(new String[]{"put","key1"}, "put key1");
        assertEquals("EchoClient> " + KVMessage.StatusType.DELETE_SUCCESS.toString().toLowerCase(Locale.ROOT) + " key1", outContent.toString().replace("\r\n",""));

        try {
            activeConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
