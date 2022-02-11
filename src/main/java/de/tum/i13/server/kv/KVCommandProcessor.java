package de.tum.i13.server.kv;

import de.tum.i13.ecs.ServerCommunicationPortData;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.SERVER_STOPPED;

public class KVCommandProcessor implements CommandProcessor {
    private KVStoreImpl kvStore;
    public static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    public KVCommandProcessor(KVStoreImpl kvStore) {
        this.kvStore = kvStore;
        logger.setLevel(StartSimpleNioServer.loggerLevel);
    }



    @Override
    public String process(String command) {

        List<String> commands = Arrays.asList(command.split(" "));
        Integer size = commands.size();
        commands.set(size - 1, commands.get(size - 1).replace("\r\n", ""));
        commands.set(0, commands.get(0).toLowerCase());

        if(kvStore.getIsStopped())
            return SERVER_STOPPED;
        logger.info("Received command on kvcommand processor: "  + command);
        if (commands.get(0).equals("get")) {
            if (size >= 3 || size == 1) {
                return (new StringBuilder()).append("error Command").append("\r\n").toString();
            } else
                return kvStore.get(commands.get(1));


        } else if (commands.get(0).equals("put")) {
            if (size < 4) {
                return (new StringBuilder()).append("error Command").append("\r\n").toString();
            } else if(commands.get(1).startsWith("-t")){
                // case topic is specified: Example of an expected command put -t:topic key value
                return kvStore.put(commands.get(2), (new StringBuilder()).append(commands.get(1)).append(" ").append(String.join(" ", commands.subList(3, commands.size()))).toString());
            }else{
                // case topic is specified: Example of an expected command put anything key value
                return kvStore.put(commands.get(2), String.join(" ", commands.subList(3, commands.size())));
            }



        } else if (commands.get(0).equals("delete")) {
            return kvStore.remove(commands.get(1));
        } else if (commands.get(0).equals("keyrange")){
            return kvStore.keyRange();
        } else if (commands.get(0).equals("keyrange_read")){
            return kvStore.keyRangeRead();
            } else {
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
