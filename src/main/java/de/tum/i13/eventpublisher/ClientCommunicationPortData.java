package de.tum.i13.eventpublisher;

import de.tum.i13.ecs.CommunicationPort;
import de.tum.i13.server.filestorage.FileStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/***
 * Used by eventPublisher to keep track of subscribers
 */
public class ClientCommunicationPortData {

    Map<String, Map<String ,SubscriberData>> keySubscriptions;
    Map<String, Map<String ,SubscriberData>> topicSubscriptions;

    private static Logger logger = Logger.getLogger(ClientCommunicationPortData.class.getName());


    public ClientCommunicationPortData() {

        this.keySubscriptions = new HashMap<>();
        this.topicSubscriptions = new HashMap<>();
    }

    public void addServerData(boolean isTopic, String ip, String port, String key){
        Map<String, Map<String ,SubscriberData>> usedMap = isTopic ? topicSubscriptions : keySubscriptions;
        logger.info("user " + ip + ":" + port + " is subscribing to " + (isTopic?"topic":"key") + " " + key);
        if(usedMap.get(key)==null)
            usedMap.put(key,new HashMap<>());
        usedMap.get(key).put(generateServerKey(ip,port),new SubscriberData(ip,port,isTopic));

    }

    public SubscriberData removeServerData(boolean isTopic, String ip, String port,String key){
        Map<String, Map<String ,SubscriberData>> usedMap = isTopic ? topicSubscriptions : keySubscriptions;
        try{
            return usedMap.get(key).remove(generateServerKey(ip,port));
        } catch (Exception e){
            logger.severe("Exception when user tried to unsubscribe ! probably fake unsubscribe called");
            return null;
        }
    }

    public  Boolean checkExists(String ip, String port,String key){
        return keySubscriptions.get(key)!=null && keySubscriptions.get(key).get(generateServerKey(ip,port))!=null;
    }

    public Boolean keyExists(String key){
        return keySubscriptions.get(key)!=null;
    }

    private String generateServerKey(String ip, String port){return ip + ":" + port;}

    public  Map<String,SubscriberData> getKeySubscribers(String key){
        return keySubscriptions.getOrDefault(key, new HashMap<>());
    }

    public  Map<String,SubscriberData> getTopicSubscribers(String topic){
        return topicSubscriptions.getOrDefault(topic, new HashMap<>());
    }

    public Map<String, Map<String, SubscriberData>> getTopicSubscriptions() {
        return topicSubscriptions;
    }
}
