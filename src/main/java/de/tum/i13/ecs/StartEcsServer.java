package de.tum.i13.ecs;

import de.tum.i13.ecs.ecsprocessor.EcsCmdProcessor;
import de.tum.i13.ecs.ecsprocessor.EcsImplementation;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartEcsServer {

    public static Logger logger = Logger.getLogger(StartEcsServer.class.getName());
    public static Level loggerLevel = Level.INFO;

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);

        //Replace with your Key Value command processor
        setLoggerLevel(cfg.logLevel);
        loggerLevel = logger.getLevel();

        EcsImplementation ecsImplementation = new EcsImplementation();
        //now we start the server for communication with the ecs
        CommandProcessor ecsLogic = new EcsCmdProcessor(ecsImplementation); //Todo : change
        SimpleEcsServer ecsSn = new SimpleEcsServer(ecsLogic); //for communication with the server
        logger.info("Listening to address  : " + cfg.listenaddr + ":" + cfg.port);
        ecsSn.bindSockets(cfg.listenaddr, cfg.port); //binds to a different port

        ecsSn.start();
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

}
