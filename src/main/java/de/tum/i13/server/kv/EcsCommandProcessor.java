package de.tum.i13.server.kv;

import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.YOUR_RANGE;

public class EcsCommandProcessor implements CommandProcessor {
    private KVStoreImpl kvStore;
    public static Logger logger = Logger.getLogger(EcsCommandProcessor.class.getName());

    public EcsCommandProcessor(KVStoreImpl kvStore) {
        this.kvStore = kvStore;
        logger.setLevel(StartSimpleNioServer.loggerLevel);
        //shutodown hook
        Thread shutdownHook = new Thread(() -> {
            try {
                logger.info("Shutdown triggered");
                Socket socket = new Socket(kvStore.getEcsHost(), kvStore.getEcsPort());
                PrintWriter output = new PrintWriter(socket.getOutputStream());
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String connectionSuccess = input.readLine(); // connection success message
                output.flush();
                output.write("removeserver " + kvStore.getMyadress() + " " + kvStore.getMyport() + "\r\n"); //todo : change adress to reflect this serve's host
                output.flush();
                String answer = input.readLine(); // we wait for answer (we should receive handoff ...)
                logger.info(answer);
                if (answer.replace("\r\n", "").startsWith(Constants.ERROR)){
                    output.close();
                    socket.close();
                    return;
                }
                process(answer); //process the answer like a typical command. TODO: HERE ALL KEYS MUST BE TRANSMITTED TO RECEIVED SERVER BUT COMMAND IS HANDOFF (dunno how to tell it to send all keys)
//                Thread.sleep(1000);
                output.close(); //after processing is done server can die
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    @Override
    public String process(String command) {


            List<String> commands = Arrays.asList(command.split(" "));
            Integer size = commands.size();
            commands.set(size - 1, commands.get(size - 1).replace("\r\n", ""));
            commands.set(0, commands.get(0).toLowerCase());
            
            logger.info("Received command on ecs processor: "  + command);
            if (commands.get(0).equals("receive")) {
                return kvStore.receive(commands);
            } else if (commands.get(0).equals("ping")){
                return "pong\r\n";
            } else if (commands.get(0).equals("handoff") && commands.size()>=4) {

                if (commands.get(3).equals(YOUR_RANGE))
                    return kvStore.handoffReplicaKeys(commands);
                else
                    return kvStore.handoff(commands);

            } else if (commands.get(0).equals("remove_duplication") && commands.size()>=2) {
                return kvStore.removeDuplication(commands);
            } else if (commands.get(0).equals("metadata_update")) {
                return kvStore.metadataUpdate(commands.get(1));
            }  else if (commands.get(0).equals(Constants.PUT_KEY_REPLICA)) {
                kvStore.putKeyReplica(commands.get(1), String.join(" ", commands.subList(2, commands.size())));
                return null;
            }  else if (commands.get(0).equals(Constants.REMOVE_KEY_REPLICA)) {
                kvStore.removeKeyReplica(commands.get(1));
                return null;
            }
            else {
                return (new StringBuilder()).append("error Command").append("\r\n").toString();

            }
        }


    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        StartSimpleNioServer.logger.info("new connection: " + remoteAddress.toString());
        return "Connection to database server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        StartSimpleNioServer.logger.info("connection closed: " + address.toString());
    }
}
