package de.tum.i13.shared;

import de.tum.i13.ecs.CommunicationPort;
import de.tum.i13.server.kv.KVStoreUtil;
import de.tum.i13.server.nio.StartSimpleNioServer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.logging.Logger;

public class MetaData {

    private TreeMap<String, ServerData> serverDataMap = new TreeMap<>();
    private CommunicationPort eventPublisher = null;
    public static Logger logger = Logger.getLogger(MetaData.class.getName());
    /**
     * Tries to add a server
     * @param ip
     * @param port
     * @return true if the server was added, false if the server already exists
     */
    public ServerData addServer(String ip, String port) {
        String hash = hashServerHostIp(ip, port);
        //Houssein explanation (delete later), to avoid problems with the ping and a server leaving at the same time (no need to send twice data)
        if (serverDataMap.containsKey(hash))
            return null;
        ServerData newServerData = new ServerData( Integer.parseInt(port), ip, getPredecessor(hash),hash);
        serverDataMap.put(hash, newServerData);
        return newServerData;
    }

    /**
     * Tries to delete a server
     * @param ip
     * @param port
     * @return true if server was deleted succesfully, false if server is inexistant
     */
    public ServerData removeServer(String ip, String port) {
        String hashToRemove = hashServerHostIp(ip, port);
        ServerData serverToRemove = serverDataMap.get(hashToRemove);
        if (serverToRemove == null) //we check if server truly exists
            return null;

        Map.Entry<String, ServerData> successor = serverDataMap.higherEntry(hashToRemove);
        if(!Objects.isNull(successor)){
            if (serverDataMap.size() == 1){
                successor.getValue().setStartIndex(successor.getValue().getEndIndex());
            } else {
                successor.getValue().setStartIndex(serverToRemove.getStartIndex());
            }

        }
        serverDataMap.remove(hashToRemove);
        return serverToRemove;
    }

    public String hashServerHostIp(String ip, String port) {
        String stringToHash = new StringBuilder().append(ip).append(':').append(port).toString();
        return hashString(stringToHash);

    }

    /**
     * Hashs a string using the md5 algorithm
     * @param stringToHash string to hash
     * @return
     */
    public static String hashString(String stringToHash){
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(stringToHash.getBytes());
            return Utils.byteToHex(messageDigest);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** transforms each server data to this format
     * <range_from>,<range_to>,<ip:port>;<range_from>,<range_to>,<ip:port>; ...
     * @return : string of all servers
     */
    public String serializeToString(){
        if (serverDataMap == null || serverDataMap.size() == 0)
            return "";

        StringBuilder builder = new StringBuilder();
        for (ServerData server : serverDataMap.values()){
            builder.append(server.getStartIndex() + "," + server.getEndIndex() + "," + server.getIp() + ":" +  server.getPort() + ";");
        }

        // we append the event publisher
        if (eventPublisher != null)
            builder.append("-ep:" + eventPublisher.getIp() +"," + eventPublisher.getPort() + ";");

        builder.append("\r\n");
        return builder.toString();
    }


    /** transforms each server data to this format
     * <range_from>,<range_to>,<ip:port>;<range_from>,<range_to>,<ip:port>; ...
     * @return : string of all servers
     */
    public String serializeDupToString(){
        if (serverDataMap == null || serverDataMap.size() == 0)
            return "";

        StringBuilder builder = new StringBuilder();
        for (ServerData server : serverDataMap.values()){
            String[] indexes = KVStoreUtil.getRangeWithDuplication(server.getIp(),String.valueOf(server.getPort()),this);
            builder.append(indexes[0] + "," + indexes[1] + "," + server.getIp() + ":" +  server.getPort() + ";");
        }

        builder.append("\r\n");
        return builder.toString();
    }

    /**
     * Transforms a string in format <range_from>,<range_to>,<ip:port>;<range_from>,<range_to>,<ip:port>; <eventprocessor ip>,<eventprocessor port>;
     * to metadata data
     * @param serializedObject
     */
    public void unserializeMetaData(String serializedObject){
        serializedObject.replace("\r\n","");

        //first we split the normal servers and the event processor
        String[] metaDataParts = serializedObject.split("\\s+|,\\s*|;\\s*");

        logger.info("unserializing metadata");
        //event publisher starts with -ep:
        serverDataMap = new TreeMap<>();
        eventPublisher = null;
        for (int i = 0; i < metaDataParts.length-1; i += 3){
            //we populate the server MetaData
            if (metaDataParts[i].startsWith("-ep:")){
                logger.info("parsing eventpublisher");
                //here we parse the event publisher
                String hostname = metaDataParts[i].substring(4);
                String port = metaDataParts[i+1];
                eventPublisher = new CommunicationPort(hostname, port);
                continue;
            }

            //HERE We parse a storage server
            String startIndex = metaDataParts[i];
            String endIndex = metaDataParts[i+1];
            String[] ipAndPort= metaDataParts[i+2].split(":");
            String ip = ipAndPort[0];
            int port = Integer.parseInt(ipAndPort[1]);
            ServerData serverMetaData = new ServerData(port, ip, startIndex, endIndex);

            //we get the ip:stirng hash
            String hash = hashServerHostIp(ip, ""+port);
            serverDataMap.put(hash, serverMetaData);
        }
    }

    /**
     * Get the server predecessor
     * @param hash : server hash we want the predecessor of
     * @return
     */
    private String getPredecessor(String hash){
        Map.Entry<String, ServerData> predecessor = serverDataMap.lowerEntry(hash);
        if(Objects.isNull(predecessor)){
            if (serverDataMap.lastEntry() != null){
                return serverDataMap.lastEntry().getKey();
            } else {
                return hash;
            }
        }
        return predecessor.getKey();
    }

    /**
     * Get the server that succeeds this server with the hash
     * @param hash : hash of the server we want to successor of
     * @return
     */
    public ServerData getServerSuccessor(String hash){
        if (serverDataMap.size() <= 1)
            return null;

        Map.Entry<String, ServerData>  successorEntry = serverDataMap.higherEntry(hash);
        if (successorEntry == null)
            return serverDataMap.firstEntry().getValue();

        return successorEntry.getValue();
    }



    /**
     * Get the next server that succeeds this server with the hash
     * @param hash : hash of the server we want to next successor of
     * @return
     */
    public ServerData getServerNextSuccessor(String hash){
        if (serverDataMap.size() <= 1)
            return null;

        Map.Entry<String, ServerData>  successorEntry = serverDataMap.higherEntry(hash);
        if (successorEntry == null)
            successorEntry= serverDataMap.firstEntry();

        Map.Entry<String, ServerData>  nextSuccessorEntry = serverDataMap.higherEntry(successorEntry.getKey());
        if (nextSuccessorEntry  == null)
            return serverDataMap.firstEntry().getValue();

        return nextSuccessorEntry.getValue();
    }





    /**
     * Get the server that precedes this server with the hash
     * @param hash : hash of the server we want to next predecessor of
     * @return
     */

    public ServerData getServerPredecessor(String hash){
        if (serverDataMap.size() <= 1)
            return null;

        ServerData predecessor = null;
        Map.Entry<String, ServerData> predecessorEntry = serverDataMap.lowerEntry(hash);
        if (predecessorEntry == null)
            return serverDataMap.lastEntry().getValue();

        predecessor = predecessorEntry.getValue();

        return predecessor;
    }



    /**
     * Get the next server that precedes this server with the hash
     * @param hash : hash of the server we want to next predecessor of
     * @return
     */
    public ServerData getServerNextPredecessor(String hash){
        if (serverDataMap.size() <= 1)
            return null;

        ServerData nextPredecessor = null;
        Map.Entry<String, ServerData> predecessorEntry = serverDataMap.lowerEntry(hash);
        if (predecessorEntry == null)
            predecessorEntry= serverDataMap.lastEntry();

        Map.Entry<String, ServerData> nextPredecessorEntry = serverDataMap.lowerEntry(predecessorEntry.getKey());
        if (nextPredecessorEntry == null)
            return serverDataMap.lastEntry().getValue();

        nextPredecessor = nextPredecessorEntry.getValue();

        return nextPredecessor;
    }




    /**
     * Hashes Ip and port and returns the server associated in the map
     * @param ip
     * @param port
     * @return
     */
    public ServerData getServerFromIdentifiers(String ip, String port){
        String hash = hashServerHostIp(ip, port);
        return serverDataMap.get(hash);
    }

    private String getEndIndex(String ip, String port){
        return "stuuf";
    }

    public TreeMap<String, ServerData> getServerDataMap() {
        return serverDataMap;
    }

    public CommunicationPort getEventPublisher(){
        return eventPublisher;
    }

    public void setEventPublisher(CommunicationPort eventPublisher) {
        this.eventPublisher = eventPublisher;
    }
}
