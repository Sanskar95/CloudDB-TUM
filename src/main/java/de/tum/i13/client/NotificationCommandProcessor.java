package de.tum.i13.client;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class NotificationCommandProcessor implements CommandProcessor {

    private final static Logger logger = Logger.getLogger(NotificationCommandProcessor.class.getName());
    private NotificationImpl notificationImpl;
    private Boolean isTest;

    public NotificationCommandProcessor(NotificationImpl notificationImpl, Boolean isTest) {
        this.notificationImpl = notificationImpl;
        this.isTest=isTest;
    }

    @Override
    public String process(String command) {
        List<String> commands = Arrays.asList(command.split(" "));
        logger.info("received command : " + command);
        if (commands.size() == 3) {
            String impactedTopic = commands.get(1);
            String impactedKey = commands.get(0);
            String impactedValue = commands.get(2);
            notificationImpl.saveImpactedTopicKeyValue(impactedKey, impactedTopic, impactedValue);
            String message = impactedValue.equals("null" + Constants.END_OF_PACKET) ? "In topic \"" + impactedTopic + "\" the key \"" + impactedKey + "\" has been deleted" : "In topic \"" + impactedTopic + "\" the key \"" + impactedKey + "\" has been changed to value \"" + impactedValue + "\"";
            System.out.println(message);

            if(isTest)
            {
                if (Milestone1Main.files.length == notificationImpl.getTopicChangeHistory().get(impactedTopic).size()) {
                    long firstEntry = notificationImpl.getTopicChangeHistory().get(impactedTopic).entrySet().stream().findFirst().get().getValue().get(0).entrySet().stream().findFirst().get().getKey();
                    long lastEntry = notificationImpl.getTopicChangeHistory().get(impactedTopic).entrySet().stream().reduce((first, second) -> second).get().getValue().get(0).entrySet().stream().findFirst().get().getKey();
                    long duration = lastEntry - firstEntry;
                    System.out.println("Duration Elapsed to recieve all notifications: " + duration);
                }
            }




        } else {
            String impactedKey = commands.get(0);
            String impactedValue = commands.get(1);
            notificationImpl.saveImpactedKeyValue(impactedKey, impactedValue);
            String message = impactedValue.equals("null" + Constants.END_OF_PACKET) ? "the key \"" + impactedKey + "\" has been deleted" : "the key \"" + impactedKey + "\" has been changed to value \"" + impactedValue + "\"";
            System.out.println(message);

            if(isTest){
                if (Milestone1Main.files.length == notificationImpl.getKeyChangeHistory().size()) {
                    long firstEntry = notificationImpl.getKeyChangeHistory().entrySet().stream().findFirst().get().getValue().get(0).entrySet().stream().findFirst().get().getKey();
                    long lastEntry = notificationImpl.getKeyChangeHistory().entrySet().stream().reduce((first, second) -> second).get().getValue().get(0).entrySet().stream().findFirst().get().getKey();
                    long duration = lastEntry - firstEntry;
                    System.out.println("Duration Elapsed to recieve all notifications: " + duration);
                }
            }

        }

        System.out.print("EchoClient> ");
        return null;

    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        return "connected to client " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {

    }
}
