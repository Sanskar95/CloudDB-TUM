package de.tum.i13.eventpublisher;

import de.tum.i13.server.kv.EcsCommandProcessor;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreImpl;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.Constants.ADD_EVENT_PUBLISHER;
import static de.tum.i13.shared.Constants.METADA_Received;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartEventPublisher {

    public static Logger logger = Logger.getLogger(StartEventPublisher.class.getName());
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


        //we start the event processor
        EventPublisherImpl kvs = new EventPublisherImpl(cfg.eventPubThreads);
        CommandProcessor logic = new EventPubCommandProcessor(kvs);
        SimpleEventPublisher sn = new SimpleEventPublisher(logic);
        sn.bindSockets(cfg.listenaddr, cfg.port);

        //we inform ecs that this is the event publisher
        contactEcs(ecsHost, cfg.bootstrap.getPort(), cfg.listenaddr, cfg.port);

        sn.start();
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


    /**
     * Contacts ecs to inform it this server was started
     * @param ecsHost
     * @param ecsPort
     * @param myAdress
     * @param myPort
     */
    public static void contactEcs(String ecsHost, int ecsPort, String myAdress, int myPort) {

        try {
            logger.info("connecting to " + ecsHost + " : " + ecsPort);
            Socket socket = new Socket(ecsHost, ecsPort);
            PrintWriter output = new PrintWriter(socket.getOutputStream());
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String confirmation = input.readLine();
            logger.info(confirmation);
            output.write(ADD_EVENT_PUBLISHER + " " + myAdress + " " + myPort + "\r\n");
            output.flush();
            String answer = input.readLine();
            logger.info(answer);
            output.flush();
            output.close();
            input.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
