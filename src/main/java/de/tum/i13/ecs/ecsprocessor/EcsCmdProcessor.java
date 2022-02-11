package de.tum.i13.ecs.ecsprocessor;

import de.tum.i13.ecs.StartEcsServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.METADA_Received;

public class EcsCmdProcessor implements CommandProcessor {

    private EcsImplementation ecsImplementation;
    public static Logger logger = Logger.getLogger(EcsCmdProcessor.class.getName());

    public EcsCmdProcessor(EcsImplementation ecsImplementation) {

        this.ecsImplementation = ecsImplementation;
        logger.setLevel(StartEcsServer.loggerLevel);
    }

    @Override
    public String process(String command) {
        logger.info("received command " + command);
        List<String> commands = Arrays.asList(command.split(" "));
        Integer size = commands.size();
        commands.set(size - 1, commands.get(size - 1).replace("\r\n", ""));
        commands.set(0, commands.get(0).toLowerCase());


        if(commands.get(0).equals("addserver")){
            //add the server to meta data and update it
            logger.info("Ok add server command received");
            String ip = commands.get(1);
            String port = commands.get(2);
            return ecsImplementation.addServer(ip, port);
        }
        else if(commands.get(0).equals("removeserver")) {
            String ip = commands.get(1);
            String port = commands.get(2);
            return ecsImplementation.removeServer(ip, port, true);
        } else if (commands.get(0).equals(METADA_Received)){
            //added server received metadata, start transfer
            String ip = commands.get(1);
            String port = commands.get(2);
            ecsImplementation.requestKeyTransmission(ip, port, false);
            return null;
        } else if (commands.get(0).equals(Constants.DONE) || commands.get(0).equals(Constants.ERROR)){
            ecsImplementation.sendMetadataToAllServers();
            ecsImplementation.sendReplication();
            return Constants.OK + "\r\n";
        } else if (commands.get(0).equals(Constants.ADD_EVENT_PUBLISHER)){
            //event publisher
            String hostname = commands.get(1);
            String port = commands.get(2);
            return ecsImplementation.registerEventPublisher(hostname, port);
        }

        return Constants.ERROR;

    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        return "Connection to ecs server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        //StartSimpleNioServer.logger.info("connection closed: " + address.toString());
    }
}
