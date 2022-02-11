package de.tum.i13.eventpublisher;

import de.tum.i13.ecs.CommunicationPort;
import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventPublisherImpl {

    private ClientCommunicationPortData clientCommunicationPortData;
    private static Logger logger = Logger.getLogger(EventPublisherImpl.class.getName());

    //these threads will work in parallel to deliver subscriptions
    private final int totalThreads;

    public EventPublisherImpl(int workerThreads){
        totalThreads = workerThreads;
        this.clientCommunicationPortData= new ClientCommunicationPortData();
    }

    /**
     * Subscribes to a certain key or topic.
     * @param ip ip adress of the client
     * @param port port of the client
     * @param flag indicates if this is a key or topic subscription
     * @param key the key or topic
     * @return success/failure string
     */
    public String subscribe(String ip, String port, String flag, String key){
        boolean isTopic = flag.equals("-t");
        clientCommunicationPortData.addServerData(isTopic, ip,port,key);

        return("Successfully subscribed to " + key + " \r\n");
    }


    /**
     * Unsubscribes from a certain key or topic.
     * @param ip ip adress of the client
     * @param port port of the client
     * @param flag indicates if this is a key or topic subscription
     * @param key the key or topic
     * @return success/failure string
     */
    public String unSubscribe(String ip, String port, String flag, String key){
        boolean isTopic = flag.equals("-t");
        SubscriberData result = clientCommunicationPortData.removeServerData(isTopic, ip, port, key);
        return (result == null ) ? ("Not subscribed to " +key+ " \r\n") : ("Succesfully unsubscribed from " +key+ " \r\n") ;
    }


    /**
     * gets all subscriber to a certain topic/key and send the update to all users
     * @param commands
     * @return
     */
    public String notifySubscribers(List<String> commands){

        //first we parse the commands
        String key = commands.get(1);
        String topic = null;
        String value = null;
        //we see if there is a topic
        if (commands.get(2).startsWith("-t:")){
            topic = commands.get(2).substring(3); //we remove the -t: from the topic
            value = String.join(" ", commands.subList(3, commands.size())); //rest is value
        } else {
            //everything is value
            value = String.join(" ", commands.subList(2, commands.size()));
        }

        //i set them to const to use them inside lambda
        final String valueConst = value;
        final String topicConst = topic;
        //Search the subscribes and notifies the affected ones
        //we merge the maps of key and topic subscribers and stream them,
        Stream<Map.Entry<String, SubscriberData>> mergedStream = Stream.concat(clientCommunicationPortData.getKeySubscribers(key).entrySet().stream(),
                clientCommunicationPortData.getTopicSubscribers(topic).entrySet().stream());


        logger.info("informing all subscribers to " + ((topicConst == null ) ? valueConst : topicConst) + " about everything");
        //now we iterate the stream and we call each subscriber
        mergedStream.forEach(entry -> {

            String hostname = entry.getValue().getIp();
            String port = entry.getValue().getPort();
            logger.info("sending subscription news to " + hostname + ":" + port);
            boolean isTopic = entry.getValue().isTopic();
            logger.info("subscribers to food are : " + clientCommunicationPortData.getTopicSubscribers("food"));
            try {
                Socket socket = new Socket(hostname, Integer.parseInt(port));
                PrintWriter outputStream = new PrintWriter(socket.getOutputStream());
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String connectionConfirmation = input.readLine();
                //if this is a topic subscription we set the topic
                String message = key + " " + ((isTopic && topicConst != null) ? topicConst + " " : "") + valueConst + "\r\n";
                outputStream.write( message);
                outputStream.flush();
                outputStream.close();
                input.close();
                socket.close();
            }catch (IOException e){
                logger.warning("Failed to contact client " + hostname + ":" + port + ". skipping ");
                //TODO : maybe on failure remove subscriber from list
            }

        });
        return null;
    }


    /**
     * Chunks a stream into equal sized Lists
     * @param stream
     * @param chunkSize
     * @param <T>
     * @return
     */
    public  <T> Stream<List<T>> chunkStream(Stream<T> stream, int chunkSize) {
        AtomicInteger index = new AtomicInteger(0);

        return stream.collect(Collectors.groupingBy(x -> index.getAndIncrement() / chunkSize))
                .entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue);
    }

    /**
     * Returns all available topics
     * @return
     */
    public String getTopics() {

        final Map<String, Map<String, SubscriberData>> topicSubscriptions = clientCommunicationPortData.getTopicSubscriptions();
        if (topicSubscriptions == null || topicSubscriptions.size() == 0){
            return "[]";
        }

        StringBuilder output = new StringBuilder();
        for (String topic : topicSubscriptions.keySet()){
            output.append(topic + ", ");
        }
        String outputstr = output.substring(0, (output.length() - 2)) + ']' + Constants.END_OF_PACKET;

        return outputstr;


    }
}
