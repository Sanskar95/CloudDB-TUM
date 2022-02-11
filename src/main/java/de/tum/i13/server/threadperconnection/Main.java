package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVStoreImpl;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {
    public static Logger logger = Logger.getLogger(Main.class.getName());


    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        logger.info("Config: " + cfg.toString());

        logger.info("starting server");

        //Replace with your Key Value command processor
        if(cfg.cachedisplacement == null) {
            cfg.cachedisplacement = "FIFO";
        }
        if(cfg.cachesize <= 0) {
            cfg.cachesize = 100;
        }

        final ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Closing thread per connection kv server");
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        //bind to localhost only
        serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

        //Replace with your Key value server logic.
        // If you use multithreading you need locking
    //    KVStoreImpl kvs = new KVStoreImpl(cfg.cachesize, cfg.cachedisplacement, cfg.dataDir, null, 1, 1);
//        CommandProcessor logic = new KVCommandProcessor(kvs);
        CommandProcessor logic = new EchoLogic();




        while (true) {
            Socket clientSocket = serverSocket.accept();

            //When we accept a connection, we start a new Thread for this connection
            Thread th = new ConnectionHandleThread(logic, clientSocket);
            th.start();
        }
    }
}
