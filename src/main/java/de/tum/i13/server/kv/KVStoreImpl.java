package de.tum.i13.server.kv;

import de.tum.i13.ecs.ServerCommunicationPortData;
import de.tum.i13.server.cache.CacheImpl;
import de.tum.i13.server.exception.KeyNotFoundException;
import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import javax.lang.model.element.NestingKind;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.*;

public class KVStoreImpl implements KVStore {
    public static Logger logger = Logger.getLogger(KVStoreImpl.class.getName());

    private CacheImpl cache;
    private ConcurrentHashMap<String, String> cacheStorage = new ConcurrentHashMap<>();
    private FileStorage fileStorage;
    private String ecsHost;
    private int ecsPort;
    private String myadress;
    private int myport;

    private Boolean isStopped;
    private Boolean writeLock;
    //contains server metadata
    private MetaData metaData;

    //contains all replicas for this server
    private Map<String, ServerDataPair> replicas = new HashMap<String, ServerDataPair>();
    private Object replicasMutex = new Object();

    private Object mutex = new Object();

    public KVStoreImpl(Integer cacheCapacity, String cacheDisplacementStrategy, Path path, String ecsHost, String myadress, int port, int myport) {
        this.fileStorage = new FileStorage(path);
        this.cache = new CacheImpl(cacheCapacity, cacheDisplacementStrategy, fileStorage);
        this.ecsHost = ecsHost;
        this.ecsPort = port;
        this.myport = myport;
        this.myadress = myadress;
        this.isStopped = true;
        this.writeLock = false;
        logger.setLevel(StartSimpleNioServer.loggerLevel);
    }


    public void setIsStopped(Boolean isStopped) {
        this.isStopped = isStopped;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
        if (metaData.getServerDataMap().size() == 1) //only this server is online, should be started
            isStopped = false;
    }

    public Boolean getIsStopped() {
        return isStopped;
    }

    @Override
    public String put(String key, String value) {
        synchronized (mutex) {
            if (writeLock)
                return SERVER_WRITE_LOCK;
            if (!KVStoreUtil.checkRange(metaData.getServerFromIdentifiers(myadress, String.valueOf(myport)), metaData, key))
                return SERVER_NOT_RESPONSIBLE;
            try {
                boolean doesExist = true;
                try {
                    String val = fileStorage.get(key);
                    if (val.startsWith("-t:") && !value.startsWith("-t:")) {
                        value = val.substring(0, val.indexOf(" ") + 1) + value;
                        fileStorage.put(key, value, false);
                        cache.put(key, value);
                    }else{
                        fileStorage.put(key, value, false);
                        cache.put(key, value);
                    }
                } catch (Exception knf) {
                    doesExist = false;
                    fileStorage.put(key, value, false);
                    cache.put(key, value);
                }
                if (value.startsWith("-t:") && value.indexOf(" ") != -1) {
                    String topic = value.substring(0, value.indexOf(" "));
                    value = value.substring(value.indexOf(" ") + 1);
                    informEventPublisher(PUT_SUCCESS + " " + key + " " + topic + " " + value + END_OF_PACKET);
                } else {
                    informEventPublisher(PUT_SUCCESS + " " + key + " " + value + END_OF_PACKET);
                }
                sendCommandToReplicas(PUT_KEY_REPLICA + " " + key + " " + value + END_OF_PACKET);
                if (doesExist) {
                    return (new StringBuilder()).append(KVMessage.StatusType.PUT_UPDATE.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).append("\r\n").toString();
                }
                return (new StringBuilder()).append(KVMessage.StatusType.PUT_SUCCESS.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).append("\r\n").toString();
            } catch (NotAllowedException | IOException ne) {
                return (new StringBuilder()).append(KVMessage.StatusType.PUT_ERROR.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).toString();
            }
        }
    }

    /**
     * Puts a replicated key
     *
     * @param key
     * @param value
     * @return
     */
    public String putKeyReplica(String key, String value) {
        try {
            fileStorage.put(key, value, true);
            return DONE + END_OF_PACKET;
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return ERROR + END_OF_PACKET;
        } catch (NotAllowedException e) {
            logger.severe(e.getMessage());
            return ERROR + END_OF_PACKET;
        }
    }

    @Override
    public String get(String key) {
        synchronized (mutex) {
            if (!KVStoreUtil.checkRangeWithDuplication(myadress, String.valueOf(myport), metaData, key))
                return SERVER_NOT_RESPONSIBLE;
            try {
                String value = cache.get(key);
                if (Objects.isNull(value)) {
                    value = fileStorage.get(key);
                    cache.put(key, value);
                }
                if (value.startsWith("-t:") && value.indexOf(" ") != -1)
                    value = value.substring(value.indexOf(" ") + 1);
                return (new StringBuilder()).append(KVMessage.StatusType.GET_SUCCESS.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).append(" ").append(value).append("\r\n").toString(); //with the value cache.get(key)
            } catch (IOException | KeyNotFoundException | NotAllowedException e) {
                return (new StringBuilder()).append(KVMessage.StatusType.GET_ERROR.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).append("\r\n").toString();
            }
        }


    }


    @Override
    public String remove(String key) {
        synchronized (mutex) {
            if (writeLock)
                return SERVER_WRITE_LOCK;
            if (!KVStoreUtil.checkRange(metaData.getServerFromIdentifiers(myadress, String.valueOf(myport)), metaData, key))
                return SERVER_NOT_RESPONSIBLE;
            try {
                cache.remove(key);
                String value = fileStorage.get(key);
                fileStorage.remove(key, false);
                if (value.startsWith("-t:") && value.indexOf(" ") != -1) {
                    String topic = value.substring(0, value.indexOf(" "));
                    informEventPublisher(REMOVE_SUCCESS + " " + key + " " + topic + " " + "null" + END_OF_PACKET);
                } else {
                    informEventPublisher(REMOVE_SUCCESS + " " + key + " null" + END_OF_PACKET);
                }
                sendCommandToReplicas(REMOVE_KEY_REPLICA + " " + key + END_OF_PACKET);
                return (new StringBuilder()).append(KVMessage.StatusType.DELETE_SUCCESS.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).append("\r\n").toString();
            } catch (Exception e) {
                return (new StringBuilder()).append(KVMessage.StatusType.DELETE_ERROR.toString().toLowerCase(Locale.ROOT)).append(" ").append(key).append("\r\n").toString();
            }

        }
    }

    /**
     * Removes a replicated key
     * @param key
     * @return
     */
    public String removeKeyReplica(String key) {
        try {
            fileStorage.remove(key, true);
            return DONE + END_OF_PACKET;
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return ERROR + END_OF_PACKET;
        } catch (NotAllowedException e) {
            logger.severe(e.getMessage());
            return ERROR + END_OF_PACKET;
        }
    }

    /**
     * Sends the command to replicas to add/delete a keyvalue. this uses lazy replication.
     *
     * @param command
     */
    private void sendCommandToReplicas(String command) {
        if (metaData.getServerDataMap().size() < 3)
            return;

        logger.info("sending the command to all replicas");


        new Thread(new Runnable() {
            @Override
            public void run() {
                String myServerHash = metaData.hashServerHostIp(myadress, "" + myport);
                logger.info("I'm" + myadress + " and My successors are " + metaData.getServerSuccessor(myServerHash).getIp() + " and " + metaData.getServerNextSuccessor(myServerHash).getIp());
                ServerData[] replicasToSendTo = new ServerData[]{metaData.getServerSuccessor(myServerHash), metaData.getServerNextSuccessor(myServerHash)};
                for (int i = 0; i < replicasToSendTo.length; i++) {
                    try {
                        if (replicas.get(replicasToSendTo[i].getIp() + ":" + replicasToSendTo[i].getPort()) == null) {
                            logger.info("couldn't find the replica's data here");
                            continue;
                        }
                        ServerDataPair replicaServerData = replicas.get(replicasToSendTo[i].getIp() + ":" + replicasToSendTo[i].getPort());
                        logger.info("Sending metadata to : " + replicasToSendTo[i].getIp() + " : " + replicasToSendTo[i].getPort());

                        Socket socket = new Socket(replicaServerData.ip, replicaServerData.port);
                        PrintWriter oos = new PrintWriter(socket.getOutputStream());
                        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String connectionConfirmation = input.readLine();
                        oos.write(command);
                        oos.flush();
                        oos.close();
                        input.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

    }


    /**
     * inform eventPublisher
     *
     * @param command
     */
    private void informEventPublisher(String command) {

        if (metaData.getEventPublisher() == null)
            return;

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Socket socket = new Socket(metaData.getEventPublisher().getIp(), Integer.parseInt(metaData.getEventPublisher().getPort()));
                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String connectionConfirmation = input.readLine();
                    writer.write(command);
                    writer.flush();
                    writer.close();
                    input.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }

    /**
     * Receives keys from another server
     *
     * @param commands : options of the command
     * @return done or error message
     */
    public String receive(List<String> commands) {
        try {
            int i = 2;
            logger.info("receiving keys");
            Boolean replication = commands.get(1).equals("r") ? true : false;
            while (i < commands.size() - 1) {
                logger.info("adding key " + commands.get(i));
                fileStorage.put(commands.get(i), commands.get(i + 1), replication);
                if (!replication)
                    cache.put(commands.get(i), commands.get(i + 1));
                i += 2;
            }
            return (new StringBuilder()).append("Done").append("\r\n").toString();
        } catch (NotAllowedException | IOException ne) {
            logger.warning("Got error exception " + ne.getMessage());
            return (new StringBuilder()).append("Error").append("\r\n").toString();
        }

    }


    /**
     * Updates the metadata after it was received from ecs
     *
     * @param newMetadata : serialized metadata
     * @return
     */
    public String metadataUpdate(String newMetadata) {

        try {
            logger.info("metadata is " + newMetadata);
            metaData.unserializeMetaData(newMetadata);
            ServerData newServerData = metaData.getServerFromIdentifiers(myadress, String.valueOf(myport));

            if (writeLock || isStopped) {
                fileStorage.update(newServerData, metaData);
                cache.update(newServerData, metaData);
            }
            isStopped = false;
            writeLock = false;
            return (new StringBuilder()).append("Got").toString();
        } catch (Exception ne) {
            return (new StringBuilder()).append("Error").toString();
        }
    }


    /**
     * This function sends back the keyRange
     *
     * @return the keyRange
     */
    public String keyRange() {
        return KEYRANGE_SUCCESS + " " + metaData.serializeToString();

    }


    /**
     * This function sends back the keyRange_read
     *
     * @return the keyRange
     */
    public String keyRangeRead() {
        return KEYRANGE_READ_SUCCESS + " " + metaData.serializeDupToString();

    }

    /**
     * This function is responsible for handing off the data.
     *
     * @param commands
     * @return a String with done when the data is handed off
     */
    // Todo delete the handed data
    public String handoff(List<String> commands) {
        try {
            writeLock = true;
            logger.info("Received handoff request");
            String message = "receive ";
            message += fileStorage.getWithRange(commands.get(3), metaData.getServerFromIdentifiers(myadress, String.valueOf(myport)));
            logger.info("Sending message to the other server : " + commands.get(1) + ":" + commands.get(2) + " the message : " + message);
            Socket socket = new Socket(commands.get(1), Integer.parseInt(commands.get(2)));
            PrintWriter outputStream = new PrintWriter(socket.getOutputStream());
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String connectionConfirmation = input.readLine();
            outputStream.write(message + "\r\n");
            outputStream.flush();

            logger.info("Waiting for handoff end");
            String answer = input.readLine().replace("\r\n", "");

            if (commands.get(3).equals(YOUR_RANGE)) {
                if (answer.equals("Done")) {
                    //we save a reference to the replica
                    String newReplicaIp = commands.get(1);
                    int newReplicaPort = Integer.parseInt(commands.get(2));
                    replicas.put(newReplicaIp + ":" + newReplicaPort, new ServerDataPair(newReplicaIp, newReplicaPort));
                }
                return null;
            } else if (answer.equals("Done")) {
                input.close();
                outputStream.close();
                socket.close();

                Socket socket2 = new Socket(ecsHost, ecsPort);
                outputStream = new PrintWriter(socket2.getOutputStream());
                input = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                connectionConfirmation = input.readLine();
                logger.info("Connection conformation" + connectionConfirmation);

                logger.info("Done executed");


                outputStream.write("Done" + "\r\n");
                outputStream.flush();
                outputStream.close();
                input.close();
                socket2.close();
                return null;
            } else {
                input.close();
                outputStream.close();
                socket.close();

                Socket socket2 = new Socket(ecsHost, ecsPort);
                outputStream = new PrintWriter(socket2.getOutputStream());
                input = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                connectionConfirmation = input.readLine();

                logger.info("Error executed");


                outputStream.write("Error" + "\r\n");
                outputStream.flush();
                outputStream.close();
                input.close();
                socket2.close();
                return (new StringBuilder()).append("Error").append("\r\n").toString();
            }
        } catch (IOException ne) {
            return (new StringBuilder()).append("Error").append("\r\n").toString();
        }
    }

    public String handoffReplicaKeys(List<String> commands) {


        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("Received handoff request");
                    String message = "receive ";
                    message += fileStorage.getWithRange(commands.get(3), metaData.getServerFromIdentifiers(myadress, String.valueOf(myport)));
                    logger.info("Sending message to the other server : " + commands.get(1) + ":" + commands.get(2) + " the message : " + message);
                    Socket socket = new Socket(commands.get(1), Integer.parseInt(commands.get(2)));
                    PrintWriter outputStream = new PrintWriter(socket.getOutputStream());
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    String connectionConfirmation = input.readLine();
                    outputStream.write(message + "\r\n");
                    outputStream.flush();

                    logger.info("Waiting for handoff end");
                    String answer = input.readLine().replace("\r\n", "");

                    if (answer != null) {
                        //we save a reference to the replica
                        String newReplicaIp = commands.get(1);
                        int newReplicaPort = Integer.parseInt(commands.get(2));
                        int newReplicaClientPort = Integer.parseInt((commands.get(4)));
                        synchronized (replicasMutex) {
                            replicas.put(newReplicaIp + ":" + newReplicaClientPort, new ServerDataPair(newReplicaIp, newReplicaPort));
                        }

                        logger.info("Replica update (add) : replicas size is " + replicas.size() + " and added server is " + newReplicaIp + ":" + newReplicaPort);
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        return null;
    }


    public String removeDuplication(List<String> commands) {

        ServerData myServerData = metaData.getServerFromIdentifiers(myadress, String.valueOf(myport));
        fileStorage.removeWithRange(commands, myServerData);

        //we remove the replica from the list
        if (commands.size() > 3) {
            String removedServerIp = commands.get(3);
            int removedServerPort = Integer.parseInt(commands.get(4));

            //logger.info("Replica update (remove) :removing duplication server : " + removedServerIp + ":" + removedServerPort);
            //replicas.remove(removedServerIp + ":" + removedServerPort);
        } else {
            //replicas = new HashMap<String, ServerDataPair>();
        }
        return null;


    }


    public String getEcsHost() {
        return ecsHost;
    }

    public int getEcsPort() {
        return ecsPort;
    }

    public int getMyport() {
        return myport;
    }

    public String getMyadress() {
        return myadress;
    }

    //contains minimal serverData
    class ServerDataPair {
        public String ip;
        public int port;

        public ServerDataPair(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }
}

