package de.tum.i13.server.nio;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.EchoConnectionBuilder;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.server.kv.EcsCommandProcessor;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreImpl;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.Constants.METADA_Received;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartSimpleNioServer {

    public static Logger logger = Logger.getLogger(StartSimpleNioServer.class.getName());
    public static Level loggerLevel;

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);

        if (cfg.cachedisplacement == null) {
            cfg.cachedisplacement = Constants.LRU;
        }
        if (cfg.cachesize <= 0) {
            cfg.cachesize = 10;
        }

        setLoggerLevel(cfg.logLevel);
        loggerLevel = logger.getLevel();
        logger.info("starting server at " + cfg.listenaddr + ":" + cfg.port);
        //Replace with your Key Value command processor

        String ecsHost = cfg.bootstrap.getHostName();
        if (ecsHost.equals("localhost"))
            ecsHost = "127.0.0.1";
        KVStoreImpl kvs = new KVStoreImpl(cfg.cachesize, cfg.cachedisplacement, cfg.dataDir, ecsHost, cfg.listenaddr,cfg.bootstrap.getPort(),cfg.port);

        CommandProcessor logic = new KVCommandProcessor(kvs);
        SimpleNioServer sn = new SimpleNioServer(logic); //this is for communication with clients
        logger.info("listening to address " + cfg.listenaddr + ":" + cfg.port);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        //now we start the server for communication with the ecs
        CommandProcessor ecsLogic = new EcsCommandProcessor(kvs); //Todo : change
        SimpleNioServer ecsSn = new SimpleNioServer(ecsLogic); //for communication with the server
        ecsSn.bindSockets(cfg.listenaddr, cfg.port + 1); //binds to a different port

        Thread clientServerThread = new Thread(sn);
        Thread ecsServerThread = new Thread(ecsSn);
        clientServerThread.start();
        ecsServerThread.start();
        addServer(ecsHost, cfg.bootstrap.getPort(), cfg.listenaddr, cfg.port,kvs);
    }

    /**
     * sets the logger to the specified level
     *
     * @param commands : the desired logger level
     */
    public static void setLoggerLevel(String commands) {
        // we set the logger level
        String loggerLevelStr = commands.toUpperCase();
        logger.info("Setting the logger level to " + loggerLevelStr);
        try {
            logger.setLevel(Level.parse(loggerLevelStr));
        } catch (Exception e) {
            logger.info("The logger level input is invalid ! please verify you spelling or type help for the allowed levels");
            return;
        }

        logger.info("The logger level has been set to " + loggerLevelStr);

    }


    public static void addServer(String ecsHost, int ecsPort, String myAdress, int myPort,KVStoreImpl kvstore) {

        try {
            logger.info("connecting to " + ecsHost + " : " + ecsPort);
            Socket socket = new Socket(ecsHost, ecsPort);
            PrintWriter output = new PrintWriter(socket.getOutputStream());
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String confirmation = input.readLine();
            logger.info(confirmation);
            output.write("addserver " + myAdress + " " + myPort + "\r\n");
            output.flush();
            String answer = input.readLine();
            logger.info(answer);
            MetaData metaData = new MetaData();
            metaData.unserializeMetaData(answer);
            logger.info("received metadata " + answer);
            kvstore.setMetaData(metaData);
            output.write(METADA_Received + " "+ myAdress + " " + myPort+"\r\n");
            output.flush();
            output.close();
            input.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
