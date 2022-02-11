package de.tum.i13;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.Milestone1Main;
import de.tum.i13.ecs.StartEcsServer;
import de.tum.i13.server.exception.KeyNotFoundException;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStoreImpl;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;
import org.junit.After;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tries to force a key transfer between server 1 and server 2 and sees if the transfer is successful.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransferKeysTest {
    private static final int totalNodes = 2;
    private static Thread[] nioThreads;
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


        nioThreads = new Thread[totalNodes];
        //
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

        //launch first server
        nioThreads[0] = new Thread() {
            @Override
            public void run() {
                try {
                    StartSimpleNioServer.main(new String[]{"-p",(NIO_PORT)+"","-d","data/0/" + "/","-b",HOST+":"+ECS_PORT });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };


        EcsThread.start();
        Thread.sleep(1000);
        nioThreads[0].start();
        Thread.sleep(2000);
        Milestone1Main.metaData = new MetaData();
    }

    @BeforeAll
    public static void launchServer() throws IOException, InterruptedException {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        launchServer(true);
    }


    /**
     * puts the first key to the started server
     */
    @Test
    @Order(1)
    public void putFirst(){
        ActiveConnection activeConnection = Milestone1Main.buildconnection(new String[]{"connect",HOST, ""+NIO_PORT});
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

    /**
     * Puts a second key to the online server
     */
    @Test
    @Order(2)
    public void putSecond(){ //puts the second key
        ActiveConnection activeConnection = Milestone1Main.buildconnection(new String[]{"connect",HOST, ""+NIO_PORT});
        assertNotNull(activeConnection);

        Milestone1Main.activeConnection = activeConnection;
        System.out.println(outContent.toString());
        outContent.reset();
        Milestone1Main.putKey(new String[]{"put","zzz", "vvalue2"}, "put zzz vvalue2");
        assertEquals("EchoClient> " + KVMessage.StatusType.PUT_SUCCESS.toString().toLowerCase(Locale.ROOT) + " zzz", outContent.toString().replace("\r\n",""));


        try {
            activeConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Start a second server and request a handoff of all keys from first server to it
     */
    @Test
    @Order(3)
    public void requestHandoff() throws IOException {
        nioThreads[1] = new Thread() {
            @Override
            public void run() {
                try {
                    StartSimpleNioServer.main(new String[]{"-p",(NIO_PORT+100)+"","-d","data/1/" + "/","-b",HOST+":"+ECS_PORT });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        nioThreads[1].start();


        Socket fakeEcsSocket = new Socket(HOST, NIO_PORT);
        PrintWriter outputStream = new PrintWriter(fakeEcsSocket.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(fakeEcsSocket.getInputStream()));
        input.readLine();
        outputStream.write("handoff " + HOST + " " + (NIO_PORT+100) + " " + Constants.SEND_ALL_KEYS);
        //we ask all keys to be handed off
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        //now we see if file contains all keys added previously
        Path partialPath = Paths.get("storage.txt");
        Path resolvedPath = Paths.get("data/1/").resolve(partialPath);
        File fileName = resolvedPath.toFile();

        try (FileReader fileReader = new FileReader(fileName)) {
            Properties prop = new Properties();
            prop.load(fileReader);
            assertNotNull(prop.getProperty("key1"));
            assertNotNull(prop.getProperty("zzz"));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @AfterAll
    public void killThreads(){
        for (int i = 0; i< nioThreads.length; i++){
            nioThreads[i].interrupt();
        }
    }
}
