package de.tum.i13.eventpublisher;


import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreImpl;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class EventPubCommandProcessor implements CommandProcessor {

    private EventPublisherImpl eventPublisherImpl;
    private static Logger logger = Logger.getLogger(EventPubCommandProcessor.class.getName());

    public EventPubCommandProcessor(EventPublisherImpl eventPublisherImpl) {
        this.eventPublisherImpl = eventPublisherImpl;
        logger.setLevel(StartSimpleNioServer.loggerLevel);
    }

    @Override
    public String process(String command) {

        List<String> commands = Arrays.asList(command.split(" "));
        Integer size = commands.size();
        commands.set(size - 1, commands.get(size - 1).replace("\r\n", ""));
        commands.set(0, commands.get(0).toLowerCase());

        logger.info("received command " + command);

        if (commands.get(0).equals(Constants.SUBSCRIBE)){
            return  eventPublisherImpl.subscribe(commands.get(1),commands.get(2),commands.get(3), commands.get(4));
        } else if (commands.get(0).equals("unsubscribe")) {
            return eventPublisherImpl.unSubscribe(commands.get(1), commands.get(2), commands.get(3), commands.get(4));
        } else if (commands.get(0).equals(Constants.PUT_SUCCESS) || commands.get(0).equals(Constants.REMOVE_SUCCESS)) {
            return eventPublisherImpl.notifySubscribers(commands);
        } else if (commands.get(0).equals("topics")) {
            return eventPublisherImpl.getTopics();
        }

        return null;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        return "Connection to event publisher server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }
}
