package de.tum.i13.ecs.ecsprocessor;

import de.tum.i13.ecs.CommunicationPort;
import de.tum.i13.ecs.MetadataService;
import de.tum.i13.ecs.ServerCommunicationPortData;
import de.tum.i13.ecs.StartEcsServer;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.*;

public class EcsImplementation {

    private MetadataService metadataService;
    private ServerCommunicationPortData serverCommunicationPortData;
    public static Logger logger = Logger.getLogger(EcsImplementation.class.getName());

    //keeps track of the last added/removed servers to trigger duplication
    private Queue<ModifiedServerData> pendingUpdatedServers = new LinkedList<ModifiedServerData>();

    /**
     * Pings all servers every 10 seconds, independant thread
     */

    public EcsImplementation() {
        metadataService = new MetadataService();
        serverCommunicationPortData = new ServerCommunicationPortData();
        logger.setLevel(StartEcsServer.loggerLevel);
        runServerPingThread();
    }

    /**
     * Pings servers every few seconds to see if they are still alive
     */
    private void runServerPingThread() {
        logger.info("starting pinging thread");
        new Thread(() -> {
            while (true) {
                logger.info("pinging all servers to see if they are still here");
                for (ServerData server : metadataService.getMetaData().getServerDataMap().values()) {
                    try {
                        String answer = writeToServerAndListen("ping\r\n", server.getIp(), server.getPort());
                    } catch (IOException e) {
                        removeServer(server.getIp(), "" + server.getPort(), false);
                        e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(1000 * 10);
                } catch (InterruptedException e) {
                    //interrupted thread sleep
                }

            }
        }).start();
    }

    /**
     * Writes to a server and doesn't listen to answer
     *
     * @param command : command written
     * @param ip      server ip
     * @param port    server port
     * @return
     */
    private void writeToServer(String command, String ip, int port) throws IOException {
        Socket serverToSend = new Socket(ip, Integer.parseInt(serverCommunicationPortData.getCommunicationPort(ip, String.valueOf(port))));
        PrintWriter oos = new PrintWriter(serverToSend.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(serverToSend.getInputStream()));
        oos.write(command);
        oos.flush();
        input.close();
        oos.close();
        serverToSend.close();
    }

    /**
     * Writes to a server and listens to the answer
     *
     * @param command : command written
     * @param ip      server ip
     * @param port    server port
     * @return
     * @throws IOException
     */
    private String writeToServerAndListen(String command, String ip, int port) throws IOException {
        Socket serverToSend = new Socket(ip, Integer.parseInt(serverCommunicationPortData.getCommunicationPort(ip, String.valueOf(port))));
        PrintWriter oos = new PrintWriter(serverToSend.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(serverToSend.getInputStream()));
        oos.write(command);
        //we read answer
        String answer = input.readLine();

        input.close();
        oos.close();
        serverToSend.close();

        return answer;
    }

    /**
     * Sends a request to update key to server affected
     *
     * @param ip              : added or removed server ip
     * @param port            : added or removed server port
     * @param isServerRemoved : if the server was added or removed
     */
    public String requestKeyTransmission(String ip, String port, boolean isServerRemoved) {
        MetaData metaData = metadataService.getMetaData();
        String serverHash = metaData.hashServerHostIp(ip, port);
        ServerData serverModified = metaData.getServerDataMap().get(serverHash);
        ServerData successor = metaData.getServerSuccessor(serverHash);
        if (successor == null) {
            //there is no successor, it's the first server. We'll just send updated Metadata
            logger.info("Only 1 server is online");
            return ERROR + "\r\n";
        }
        //server that sends and server that receives
        ServerData transmittingServer = (isServerRemoved) ? serverModified : successor;
        ServerData receivingServer = (isServerRemoved) ? successor : serverModified;

        //a b c / if b was removed, c must change its start index to b / if b was added, c must change its startindex to a
        //     / b must send its data to c (if possible)              / c must send its keys to b

        logger.info("Transmitting server is null ? " + (transmittingServer == null));
        logger.info("Receiving server is null ? " + (receivingServer == null));
        if (transmittingServer != null && receivingServer != null) {
            String command = "handoff " + receivingServer.getIp() + " " + serverCommunicationPortData.getCommunicationPort(receivingServer.getIp(), String.valueOf(receivingServer.getPort())) + " "
                    + ((isServerRemoved) ? Constants.SEND_ALL_KEYS : receivingServer.getEndIndex()) + Constants.END_OF_PACKET;
            //When server is getting deleted, we do'nt put the other server startIndex, we put the send_all_keys constant because all keys must be sent
            try {
                logger.info("Trasmitting port : " + transmittingServer.getIp() + ":" + transmittingServer.getPort());
                logger.info("receiving port :" + receivingServer.getIp() + ":" + receivingServer.getPort());
                if (isServerRemoved) {
                    return command;
                } else {
                    writeToServer(command, transmittingServer.getIp(), transmittingServer.getPort());
                }
            } catch (IOException e) {
                logger.warning(e.getMessage());
                e.printStackTrace();
            }
        }

        return Constants.ERROR + "\r\n";
    }


    /**
     * Updates metadata startIndex and endIndex of neighbouring servers to the one added
     */
    private void updateMetaData(String hostname, String port, boolean isAdded) {
        MetaData metaData = metadataService.getMetaData();
        String serverHash = metaData.hashServerHostIp(hostname, port);
        //servers are are ordered by hash, a b c , if b was added => c startindex changes
        ServerData myserver = metaData.getServerFromIdentifiers(hostname, port);
        ServerData successor = metaData.getServerSuccessor(serverHash);
        if (successor == null)
            return;
        //we update the successor's endindex
        if (isAdded)
            successor.setStartIndex(serverHash);
        else
            successor.setStartIndex(myserver.getStartIndex());
    }

    /**
     * Sends updated metadata to all servers
     */
    public void sendMetadataToAllServers() {
        MetaData metaData = metadataService.getMetaData();
        for (Map.Entry<String, ServerData> entry : metaData.getServerDataMap().entrySet()) {
            ServerData serverData = entry.getValue();
            Socket serverToSend = null;
            try {
                int communicationPort = Integer.valueOf(serverCommunicationPortData.getCommunicationPort(serverData.getIp(), "" + serverData.getPort()));
                logger.info("Sending metadata to : " + serverData.getIp() + " : " + communicationPort);
                serverToSend = new Socket(serverData.getIp(), communicationPort);
                PrintWriter oos = new PrintWriter(serverToSend.getOutputStream());
                BufferedReader input = new BufferedReader(new InputStreamReader(serverToSend.getInputStream()));
                String connectionConfirmation = input.readLine();
                oos.write("metadata_update " + metaData.serializeToString());
                oos.flush();
                oos.close();
                input.close();
                serverToSend.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * execute replication step after a server has been added or removed
     */
    public void sendReplication(){
        //we get the server that was added or removed
        ModifiedServerData changedServer = pendingUpdatedServers.poll();

        logger.info("changed server is " + changedServer.serverData.getIp() + ":" + changedServer.serverData.getPort() + " and it was added ? " + changedServer.wasAdded);
        MetaData metaData = metadataService.getMetaData();
        logger.info("requesting replication to be done");
        logger.info("actual metadata is " + metaData.serializeToString());
        String serverHash = metaData.hashServerHostIp(changedServer.serverData.getIp(), changedServer.serverData.getPort()+"");

        if (changedServer.wasAdded){
            logger.info("last server was added");
            if (metaData.getServerDataMap().size() < 3){
                logger.info("requesting replication to be done");
                return;
            }
            else if (metaData.getServerDataMap().size() == 3){
                /**3 servers only, each send to other 2 **/
                logger.info("metadata size is 3");
                ServerData[] servers = new ServerData[]{changedServer.serverData, metaData.getServerSuccessor(serverHash), metaData.getServerNextSuccessor(serverHash)};
                for (int i = 0; i< servers.length; i++){
                    requestKeyTransmissionReplication(servers[i].getIp(), servers[i].getPort(), servers[(i+1)%3].getIp(), servers[(i+1)%3].getPort());
                    requestKeyTransmissionReplication(servers[i].getIp(), servers[i].getPort(), servers[(i+2)%3].getIp(), servers[(i+2)%3].getPort());
                }
            }
            else{
                logger.info("Adapting to the server metadata changes");
                /**
                /*Server was added : let's say the server 1,2,3,4,5 (and 3 was the added one)
                /*1 and 2 must send their keys to 3
                /*5 must remove 2's keys, 4 must remove 1's keys.
                /*3 must send its keys to 4 and 5
                 I'll use this example for variable names
                 **/
                ServerData server1 = metaData.getServerNextPredecessor(serverHash);
                ServerData server2 = metaData.getServerPredecessor(serverHash);
                ServerData server3 = changedServer.serverData; // new server
                ServerData server4 = metaData.getServerSuccessor(serverHash);
                ServerData server5 = metaData.getServerNextSuccessor(serverHash);

                //first predecessors send to new server
                requestKeyTransmissionReplication(server1.getIp(), server1.getPort(), server3.getIp(), server3.getPort());
                requestKeyTransmissionReplication(server2.getIp(), server2.getPort(), server3.getIp(), server3.getPort());

                //now new server sends to successors
                requestKeyTransmissionReplication(server3.getIp(), server3.getPort(), server4.getIp(), server4.getPort());
                requestKeyTransmissionReplication(server3.getIp(), server3.getPort(), server5.getIp(), server5.getPort());

                //now successors remove their keys
                removeKeyRange(server4.getIp(), server4.getPort(), server1);
                removeKeyRange(server5.getIp(), server5.getPort(), server2);

            }



        }else{
            //server was removed
            logger.info("last server was removed");
            if (metaData.getServerDataMap().size() < 2){
                return;
            }
            if (metaData.getServerDataMap().size() == 2){
                //no more replication, we delete all replica keys
                logger.info("Too little servers ! removing replication");
                for (Map.Entry<String, ServerData> entry : metaData.getServerDataMap().entrySet()) {
                    removeKeyRange(entry.getValue().getIp(), entry.getValue().getPort(), ONLY_MY_RANGE, "");
                }
            } else {
                /**
                 * Example : we have server 1,2,3,4,5 and let's say server 3 was removed.
                 * Server 1 must send its keys to server 4, server 2 must send its keys to server 5
                 * Server 5 must delete the server 3 keys. Server 4 must delete the server 3 keys
                 */
                logger.info("Adapting to the server metadata changes");
                ServerData server1 = metaData.getServerNextPredecessor(serverHash);
                ServerData server2 = metaData.getServerPredecessor(serverHash);
                ServerData server3 = changedServer.serverData;
                ServerData server4 = metaData.getServerSuccessor(serverHash);
                ServerData server5 = metaData.getServerNextSuccessor(serverHash);

                requestKeyTransmissionReplication(server1.getIp(), server1.getPort(), server4.getIp(), server4.getPort());
                requestKeyTransmissionReplication(server2.getIp(), server2.getPort(), server5.getIp(), server5.getPort());
                removeKeyRange(server4.getIp(), server4.getPort(), server3);
                removeKeyRange(server5.getIp(), server5.getPort(), server3);

            }

        }
    }
    /**
     * execute replication step
     */
    /*public void sendReplication() {
        MetaData metaData = metadataService.getMetaData();

        logger.info("requesting replication to be done");
        logger.info("actual metadata is " + metaData.serializeToString());
        if (metaData.getServerDataMap().size() > metaData.getServerDataMapOldVersion().size()) {
            logger.info("metadata oldServer data map is smaller than actual metadata size");
            if (metaData.getServerDataMap().size() == 3) {
                for (Map.Entry<String, ServerData> entry : metaData.getServerDataMap().entrySet()) {

                    logger.info("Metadata size is 3");
                    ServerData currentServerData = entry.getValue();
                    ServerData successor = metaData.getServerSuccessor(entry.getKey());
                    ServerData nextSuccessor = metaData.getServerNextSuccessor(entry.getKey());
                    requestKeyTransmissionReplication(currentServerData.getIp(), currentServerData.getPort(), successor.getIp(), String.valueOf(successor.getPort()));
                    requestKeyTransmissionReplication(currentServerData.getIp(), currentServerData.getPort(), nextSuccessor.getIp(), String.valueOf(nextSuccessor.getPort()));
                }


            } else if (metaData.getServerDataMap().size() > 3) {
                logger.info("Metadata size is superior to 3");
                String hashOfAddedServer = metaData.getAddedServer();
                ServerData addedServer = metaData.getServerDataMap().get(hashOfAddedServer);
                ServerData predecessor = metaData.getServerPredecessor(hashOfAddedServer);
                ServerData nextPredecessor = metaData.getServerNextPredecessor(hashOfAddedServer);
                ServerData successor = metaData.getServerSuccessor(hashOfAddedServer);
                ServerData nextSuccessor = metaData.getServerNextSuccessor(hashOfAddedServer);

                requestKeyTransmissionReplication(addedServer.getIp(), addedServer.getPort(), successor.getIp(), String.valueOf(successor.getPort()));
                requestKeyTransmissionReplication(addedServer.getIp(), addedServer.getPort(), nextSuccessor.getIp(), String.valueOf(nextSuccessor.getPort()));
                requestKeyTransmissionReplication(nextPredecessor.getIp(), nextPredecessor.getPort(), addedServer.getIp(), String.valueOf(addedServer.getPort()));
                requestKeyTransmissionReplication(predecessor.getIp(), predecessor.getPort(), addedServer.getIp(), String.valueOf(addedServer.getPort()));
                removeKeyRange(successor.getIp(), successor.getPort(), nextPredecessor);
                removeKeyRange(nextSuccessor.getIp(), nextSuccessor.getPort(), predecessor);
            }
        } else {
            logger.info("metadata oldServer data map is bigger than actual metadata size");
            if (metaData.getServerDataMap().size() == 2) {
                logger.info("Metadata size is 2");
                for (Map.Entry<String, ServerData> entry : metaData.getServerDataMap().entrySet()) {
                    removeKeyRange(entry.getValue().getIp(), entry.getValue().getPort(), ONLY_MY_RANGE, "");
                }

            } else if (metaData.getServerDataMap().size() > 2) {
                logger.info("Metadata size is superior to 2");
                String hashOfRemovedServer = metaData.getRemovedServer();
                ServerData removedServer = metaData.getServerDataMapOldVersion().get(hashOfRemovedServer);
                ServerData predecessor = metaData.getServerPredecessorOldversion(hashOfRemovedServer);
                ServerData nextPredecessor = metaData.getServerNextPredecessorOldversion(hashOfRemovedServer);
                ServerData successor = metaData.getServerSuccessorOldVersion(hashOfRemovedServer);
                ServerData nextSuccessor = metaData.getServerNextSuccessorOldVersion(hashOfRemovedServer);

                requestKeyTransmissionReplication(nextPredecessor.getIp(), nextPredecessor.getPort(), successor.getIp(), String.valueOf(successor.getPort()));
                requestKeyTransmissionReplication(predecessor.getIp(), predecessor.getPort(), nextSuccessor.getIp(), String.valueOf(nextSuccessor.getPort()));
                removeKeyRange(successor.getIp(), successor.getPort(), removedServer);
                removeKeyRange(nextSuccessor.getIp(), nextSuccessor.getPort(), removedServer);

            }


        }
        logger.info("replication request done");

    }*/


    /**
     * Sends a request to update key to server affected
     *
     * @param sendingIp     ip of sending server
     * @param sendingPort   port of sending server
     * @param receivingIp   ip of receiving server
     * @param receivingPort port of receiving server
     */
    public String requestKeyTransmissionReplication(String sendingIp, int sendingPort, String receivingIp, int receivingPort) {

        logger.info("requesting server " + sendingIp + ":" + sendingPort + " to send their keys to " + receivingIp + ":" + receivingPort);
        String command = "handoff " + receivingIp + " " + serverCommunicationPortData.getCommunicationPort(receivingIp, receivingPort+"") + " " + YOUR_RANGE + " " + receivingPort + Constants.END_OF_PACKET;
        try {
            writeToServer(command, sendingIp, sendingPort);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return Constants.ERROR + "\r\n";
    }


    /**
     * Sends a request to remove keys  to server affected
     *
     * @param sendingIp     ip of sending server
     * @param sendingPort   port of sending server
     * @param removedServer removed server data
     */
    public String removeKeyRange(String sendingIp, int sendingPort, ServerData removedServer) {

        String startIndex = removedServer.getStartIndex();
        String endIndex = removedServer.getEndIndex();
        String serverIp = removedServer.getIp();
        String serverPort = serverCommunicationPortData.getCommunicationPort(serverIp, removedServer.getPort() + "");
        logger.info("requesting server " + sendingIp + ":" + sendingPort + " to remove duplication of server " + serverIp + ":" + removedServer.getPort());
        //predecessor.getIp(), serverCommunicationPortData.getCommunicationPort(predecessor.getIp(), predecessor.getPort()+"")
        String command = "remove_duplication " + startIndex + " " + endIndex + " " + serverIp + " " + serverPort + Constants.END_OF_PACKET;
        try {
            writeToServer(command, sendingIp, sendingPort);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return Constants.ERROR + "\r\n";
    }

    /**
     * Sends a request to remove keys  to server affected
     *
     * @param sendingIp   ip of sending server
     * @param sendingPort port of sending server
     * @param startIndex  startIndex of the range
     * @param endIndex    endIndex of the range
     */
    public String removeKeyRange(String sendingIp, int sendingPort, String startIndex, String endIndex) {

        String command = "remove_duplication " + startIndex + " " + endIndex + Constants.END_OF_PACKET;
        logger.info("requesting server " + sendingIp + ":" + sendingPort + " to remove duplication from " + startIndex + " to " + endIndex);
        try {
            writeToServer(command, sendingIp, sendingPort);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return Constants.ERROR + "\r\n";
    }

    /**
     * Tries to add a server to the metadata
     *
     * @param ip   : ip of the new server
     * @param port : port of the new server
     * @return
     */
    public String addServer(String ip, String port) {
        logger.info("server is being added " + ip + ":" + port);
        ServerData newServer = metadataService.addServer(ip, port);
        int communicationPort = Integer.valueOf(port) + 1;
        serverCommunicationPortData.addServerData(ip, port, "" + communicationPort); //TODO socket incorrect
        updateMetaData(ip, port, true);

        if (newServer != null) pendingUpdatedServers.add(new ModifiedServerData(newServer, true));

        if (metadataService.getMetaData().getServerDataMap().size() <= 1) pendingUpdatedServers = new LinkedList<ModifiedServerData>();
        //TODO : contact the two predecessors to tell them to update their duplicate to use the new server as a duplicate
        //TODO : Also pass the two successors to the new server to do the duplicate
        return metadataService.getMetaData().serializeToString();
    }


    /**
     * Attempts to remove a server
     *
     * @param ip      : ip of the server to be removed
     * @param port    : port of the server to be removed
     * @param handled : if we have communication with the server that is shutting down
     * @return
     */
    public String removeServer(String ip, String port, boolean handled) {

        String command = ERROR + "\r\n";
        if (handled)
            command = requestKeyTransmission(ip, port, true);
        updateMetaData(ip, port, false);
        ServerData removedServer = metadataService.removeServer(ip, port);
        sendMetadataToAllServers();
        if (removedServer == null) {
            return ERROR + "\r\n";
        }
        pendingUpdatedServers.add(new ModifiedServerData(removedServer, false));

        //serverCommunicationPortData.removeServerData(ip, port);
        //updateMetaData(ip, port, false);
        /*if (!handled) {
            sendMetadataToAllServers();
        } else {
            ServerData successor = metadataService.getMetaData().getServerSuccessor(metadataService.getMetaData().hashServerHostIp(ip, port));
        }*/

        //TODO : meanwhile also contact the two predesscor of this server tell them to relocate their keys
        //TODO : also tell the two successors to reblance the keys

        return command; //since the server to be removed is the one that will send the keys. We send the command to it
    }


    /**
     * Registers the event processor to the metadata
     * @param hostname
     * @param port
     * @return
     */
    public String registerEventPublisher(String hostname, String port){
        metadataService.getMetaData().setEventPublisher(new CommunicationPort(hostname, port));
        sendMetadataToAllServers();
        return DONE + "\r\n";
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public ServerCommunicationPortData getServerCommunicationPortData() {
        return serverCommunicationPortData;
    }

    /**
     * Keeps track of the last added or removed servers
     */
    class ModifiedServerData {
        public ServerData serverData;
        public boolean wasAdded;

        public ModifiedServerData(ServerData serverData, boolean wasAdded) {
            this.serverData = serverData;
            this.wasAdded = wasAdded;
        }
    }
}
