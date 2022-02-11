package de.tum.i13.client;

import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.shared.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.NOTOPICINDICATOR;


public class Milestone1Main {

    private final static Logger logger = Logger.getLogger(Milestone1Main.class.getName());

    //contains the socket communicating with the server
    public static ActiveConnection activeConnection;
    public static File [] files = new File("/home/blinmaker/TUM/cloudDB/ms5/gr10/all_documents").listFiles();

    //contains server metadata
    public static MetaData metaData;

    private static String clientHostname = "127.0.0.1";
    private static int clientPort;

    static {
        try {
            clientPort = Utils.getFreePort();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int tries = 0; //

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        metaData = new MetaData();
        ActiveConnection activeConnection = null;


        //       String[] connectArray={"connect", "127.0.0.1", "5156"};
//        String connectCommand = "connect "+ file.getAbsolutePath()+ " "+ content;

//        interpretCommand(connectArray,  "line");
//        long startTime = System.currentTimeMillis();
//
//        populateStorageService();
//        long endTime = System.currentTimeMillis();
//        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
//        System.out.println("duration "+ duration + " starttime " + startTime + " endtime "+ endTime);

        NotificationImpl kvs = new NotificationImpl();
        CommandProcessor logic = new NotificationCommandProcessor(kvs, args.length>0);
        SimpleNioServer sn = new SimpleNioServer(logic);
        sn.bindSockets(clientHostname,clientPort );
        Thread notificationReceiverThread = new Thread(sn);
        notificationReceiverThread.start();
        logger.info("Notification receiver started on the port "+  clientPort);

        if(args.length>0){
            if(args[0].equals("bench")){
                if(args[1].equals("impacted")){
                    String[] connectArray={"connect", "127.0.0.1", "5158"};
                    interpretCommand(connectArray,  "line");
                    if(args[2].equals("topic")){
                        bulkInsertKeysWithTopic(files);
                    }else{
                        bulkInsertKeysWithoutTopic(files);
                    }

                } else if(args[1].equals("impacting")){
                    String[] connectArray={"connect", "127.0.0.1", "5158"};
                    interpretCommand(connectArray,  "line");
                    bulkDeleteKeys();
                }
            }
        }



        for (; ; ) {
            System.out.print("EchoClient> ");
            String line = reader.readLine();
            String[] command = line.split(" ");
            logger.info("Received command " + command[0]);
            tries = 0; // we reset the tries

            if (command[0].equals("quit")) {
                printEchoLine("Application exit!" + "\r\n");
                return;
            }


            interpretCommand(command, line);
        }
    }

    /**
     * Interprets the first command and calls the approriate function
     *
     * @param command : should contain a key and a value.
     * @param line    : commands merged in a single string.
     */
    private static void interpretCommand(String[] command, String line) {
        tries++; //to prevent trying to loop through tests forever
//        if (tries > 5){
//            printEchoLine("failed too many times to connect to the right server, try again");
//            return;
//        }
        logger.info("interpreting the command : " + command[0]);
        switch (command[0]) {
            case "connect":
                activeConnection = buildconnection(command);
                break;
            case "send":
                sendmessage(command, line);
                break;
            case "disconnect":
                closeConnection();
                break;
            case "get":
                getKey(command, line);
                break;
            case "put":
                putKey(command, line);
                break;
            case "keyrange_read":
                keyRangeRead(command, line);
                break;
            case Constants.SUBSCRIBE:
            case "unsubscribe":
                unsubscribeOrSubscribe(command, line);
                break;
            case "logLevel":
                setLoggerLevel(command);
                break;
            case "help":
                printHelp();
                break;
            case "sb":
                bulkSubscribeToKeys();
            case "sbt":
                subscribeToTopic();
            default:
                printEchoLine("Unknown command" + "\r\n");
        }
    }


    /**
     * Prints all help commands
     */
    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("connect <address> <port> - Tries to establish a TCP- connection to the echo server based on the given server address and the port number of the echo service.");
        System.out.println("disconnect - Tries to disconnect from the connected server.");
        System.out.println("send <message> - Sends a text message to the echo server according to the communication protocol.");
        System.out.println("get <key> - Retrieves the value for the given key from the storage server.");
        System.out.println("put -t:topic <key> <value> - Inserts or updates a key-value pair into the storage server. A topic is optionnal. If value equals null deletes the key.");
        System.out.println("logLevel <level> - Sets the logger to the specified log level (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
        System.out.println("Subscribe (-t|-k) <key or topic> - subscribes to the stated key/topic depending on the flag (-t for topic, -k for key)");
        System.out.println("unsubscribe (-t|-k) <key or topic> - unsubscribes from the stated key/topic depending on the flag (-t for topic, -k for key)");
        System.out.println("help - Display this help");
        System.out.println("quit - Tears down the active connection to the server and exits the program execution.");
    }

    /**
     * sets the logger to the specified level
     *
     * @param commands : the desired logger level
     */
    public static void setLoggerLevel(String[] commands) {
        if (commands.length < 2) {
            printEchoLine("incorrect number of parameters ! please input a logger level");
            return;
        }
        // we set the logger level
        String loggerLevelStr = commands[1].toUpperCase();
        logger.info("Setting the logger level to " + loggerLevelStr);
        try {
            logger.setLevel(Level.parse(loggerLevelStr));
        } catch (Exception e) {
            printEchoLine("The logger level input is invalid ! please verify you spelling or type help for the allowed levels");
            return;
        }

        printEchoLine("The logger level has been set to " + loggerLevelStr);

    }

    /**
     * Prints a line formatted for the user interface
     *
     * @param msg : message to be displayed
     */
    public static void printEchoLine(String msg) {
        System.out.println("EchoClient> " + msg);
    }

    /**
     * Attemps to close the connection with the server
     */
    private static void closeConnection() {
        if (activeConnection != null) {
            try {
                activeConnection.close();
            } catch (Exception e) {
                logger.warning("Could not close active connection");
                activeConnection = null;
            }
        }
    }

    /**
     * Tries to send a message to the server
     *
     * @param command should contain the message to be sent
     * @param line    full command in a single string
     */
    public static void sendmessage(String[] command, String line) {
        if (activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }
        int firstSpace = line.indexOf(" ");
        if (firstSpace == -1 || firstSpace + 1 >= line.length()) {
            printEchoLine("Error! Nothing to send!");
            return;
        }

        String cmd = line.substring(firstSpace + 1);
        activeConnection.write(cmd);

        try {
            printEchoLine(activeConnection.readline());
        } catch (IOException e) {
            printEchoLine("Error! Not connected!");
        }
    }

    /**
     * Tries to retrieve a key from the server and display it to the user
     *
     * @param command should contain the key
     * @param line    full command in a single string
     */
    public static void getKey(String[] command, String line) {
        if (activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }

        if (command.length < 2) {
            printEchoLine("Please also input the key to be retrieved from the server");
            return;
        }

        connectToResponsibleServer(command[1]); // we connect to the responsible server
        //we write to the server
        activeConnection.write(line);

        InterpretServerAnswer(command, line);
    }


    /**
     * Connects to the server responsible for the given key
     *
     * @param key
     */
    public static void connectToResponsibleServer(String key) {
        ServerData responsibleServer = getServerFromkey(key);

        logger.info("responsible server is " + responsibleServer.getIp() + ":" + responsibleServer.getPort());
        //same server, no need to recreate a socket
        String serverHostName = activeConnection.getHostname();
        if (serverHostName.equals("localhost")) serverHostName = "127.0.0.1";
        if (serverHostName.equals(responsibleServer.getIp()) && activeConnection.getPort() == responsibleServer.getPort())
            return;

        // we close old connection
        try {
            activeConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        activeConnection = null;

        //we try to connect to the correct server to get the key. In the possibility of the server being closed, we try contacting the next one in
        //the metadata
        while (activeConnection == null) {
            activeConnection = buildconnection(new String[]{"connect", responsibleServer.getIp(), "" + responsibleServer.getPort()});

            if (activeConnection == null || metaData.getServerDataMap().size() == 0) {
                String outdatedServerHash = responsibleServer.getEndIndex();

                //what this should do => get the first server that has a superior index to the selected server OR get the first server in the list

                responsibleServer = metaData.getServerDataMap().values()
                        .stream().filter(server -> server.getEndIndex().compareTo(outdatedServerHash) > 0)
                        .findFirst().orElse(metaData.getServerDataMap().firstEntry().getValue());
            }
        }



        logger.info("connected to the correct server");
    }

    /**
     * searches the metadata for the server that handles a certain keyrange.
     * If metadata is unpopulated returs a random server to get keyrange
     *
     * @param key
     * @return
     */
    public static ServerData getServerFromkey(String key) {
        if (metaData.getServerDataMap().size() == 1) {
            return metaData.getServerDataMap().firstEntry().getValue();
        }

        String keyHash = metaData.hashString(key);

        //The responsible should be the first higher end index server
        Map.Entry<String, ServerData> responsibleServerEntry = metaData.getServerDataMap().higherEntry(keyHash);

        if (responsibleServerEntry == null){
            return metaData.getServerDataMap().firstEntry().getValue();
        }


        return responsibleServerEntry.getValue();
    }

    /* //if we find a hashrate higher than the one we need we stop and return last server
        boolean correctServer = false;
        for (ServerData serverData : serverList) {
            if ((serverData.getStartIndex()).compareTo(keyHash) <= 0) {
                responsableServer = serverData;
            } else {
                correctServer = true;
                break;
            }
        }

        if (!correctServer) { //basically when key is bigger than all servers, it should be the server that is used for this key
            if (responsableServer.getEndIndex().compareTo(keyHash) < 0)
                return serverList.stream().findFirst().get();
        }*/

    /**
     * Sends a key / value /topic (optionnal) to the server to be stored in the database
     *
     * @param command : should contain a key and a value.
     * @param line    : commands merged in a single string.
     */
    public static void putKey(String[] command, String line) {
        if (activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }

        if (command.length < 3 && command.length != 2) {
            printEchoLine("Please specify the topic if applicable and the key to be put in server database");
            return;
        }

        if(command[1].startsWith("-t")){
            connectToResponsibleServer(command[2]);
        }else{
            connectToResponsibleServer(command[1]);
        }
      ; // we connect to the responsible server

        if (command.length == 2){
            //the format is put key => we delete the key
            activeConnection.write("delete " + command[1]);
        }
        else if(command.length==3){
            if(command[1].startsWith("-t")){
                //format is put -t:topic key (no value, invalid input)
                printEchoLine("Please also input the  value to be put in the server database");
                return;
            }else{
                activeConnection.write(addNoTopicString(command));
            }

        }else {
            if(command[1].startsWith("-t")){
                activeConnection.write(line);
            }else {
                activeConnection.write(addNoTopicString(command));
            }
        }



        InterpretServerAnswer(command, line);

    }


    /**
     * Buils a connection to the host and port input by the user.
     *
     * @param command : user input commands (should contain hostname and port)
     * @return a socket handler with an active communication with the server
     */
    public static ActiveConnection buildconnection(String[] command) {
        if (command.length == 3) {
            try {
                String port = command[2];
                String hostname = command[1];

                //if meta data is empty we add this server to use to retrieve keys later
                if (metaData.getServerDataMap().size() == 0) {
                    metaData.addServer(hostname, port);
                }

                EchoConnectionBuilder kvcb = new EchoConnectionBuilder(hostname, Integer.parseInt(port));
                ActiveConnection ac = kvcb.connect();
                String confirmation = ac.readline();
                printEchoLine(confirmation);
                return ac;
            } catch (IllegalArgumentException e) {
                printEchoLine("Could not connect. Invalid port.");
            } catch (java.net.UnknownHostException e) {
                printEchoLine("Could not connect. ip address of the host could not be determined.");
            } catch (Exception e) {
                printEchoLine("Could not connect to server");
            }
        } else {
            printEchoLine("wrong input size ! please type the ip and port");
        }
        return null;
    }

// 3 7.1, 2 8.3, 4 6.64, 5 5.39, 1 11.014

    /**
     * Interprets the server answers and reacts accordingly to error messages
     *
     * @param command : should contain a key and a value.
     * @param line    : commands merged in a single string.
     */
    public static void InterpretServerAnswer(String[] command, String line) {

        try { //we read the server answer
            String answer = activeConnection.readline();
            printEchoLine(answer);
            if (Constants.SERVER_NOT_RESPONSIBLE.startsWith(answer)) {
                //server is not responsible, we need a metadata update
                UpdateMetaData(command, line, "keyrange");
            } else if (Constants.KEYRANGE_READ_SUCCESS.startsWith(answer)) {
                UpdateMetaData(command, line, "keyrange_read");
            }
        } catch (IOException e) {
            printEchoLine("Could not read server answer");
        }
    }

    /**
     * Prints Keyrange of the KVServers including replicas
     */

    private static void keyRangeRead(String[] command, String line) {
        if (activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }
        InterpretServerAnswer(command, line);
    }


    /**
     * Pulls the metadata from the server
     */
    public static void UpdateMetaData(String[] command, String line, String serverCommand) {
        activeConnection.write(serverCommand);

        /*<range_from>,<range_to>,<ip:port>;...
        /*
         */
        logger.info("requesting Updated metadata from server");
        try { //we read the server answer
            String answer = activeConnection.readline().replace("\r\n", "");
            //first part should contain the response code, second part the list of answers separated by , and ; (delimiters are linebreak ; and ,
            String[] answerParts = answer.split(" ");
            logger.info("received answer " + answer);
            if (Constants.KEYRANGE_SUCCESS.startsWith(answerParts[0]) && answerParts.length > 1) {
                printEchoLine(answer);
                logger.info("got keyrange success");
                metaData.unserializeMetaData(answerParts[1]);
                //then recall function first called
                interpretCommand(command, line);
            } else {
                printEchoLine(answer);
            }

        } catch (IOException e) {
            printEchoLine("Could not read server answer");
        }
    }


    /**
     * Subscribes / unsubscribes from the input key changes
     * @param command
     * @param line
     */
    public static void unsubscribeOrSubscribe(String[] command, String line) {
        //if after doing the keyrange command the eventpublisher is still null, we abort
        if (metaData.getServerDataMap().size() == 0){
            printEchoLine("please connect to a server first");
            return;
        }
        if (command.length < 3) {
            printEchoLine("Unsufficient arguments ! please write which key or topic  you want to subscribe/unsubscribe to");
            return;
        }
        if (tries >= 2 && metaData.getEventPublisher() == null) {
            printEchoLine("There is no event publisher online, aborting command");
            return;
        }

        //if eventPublisher is null, we update metadata
        if (metaData.getEventPublisher() == null) {
            if (activeConnection == null)
                connectToResponsibleServer("a");
            UpdateMetaData(command, line, "keyrange");
        }

        try {

            //we connect to the event publisher and subscribe/unsubscribe
            EchoConnectionBuilder kvcb = new EchoConnectionBuilder(metaData.getEventPublisher().getIp(), Integer.parseInt(metaData.getEventPublisher().getPort()));
            ActiveConnection connectionToEventPub = kvcb.connect();
            //we read the confirmation message and skip it
            connectionToEventPub.readline();

            logger.info("our hostname is " + activeConnection.getHostname() + " and port is " + activeConnection.getPort());
            connectionToEventPub.write(command[0] + " " + clientHostname + " " + clientPort + " " + command[1] + " "+ command[2]);
            String SubUnsubConfirmation = connectionToEventPub.readline();
            printEchoLine(SubUnsubConfirmation);
            connectionToEventPub.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Modifies a put command written by the user to make it formatted for the server parser
     * @param command
     * @return
     */
    public static String  addNoTopicString(String[] command){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(command[0]).append(" ").append(NOTOPICINDICATOR);
       for (int i=1; i<command.length; i++){
           stringBuilder.append(" ").append(command[i]);
       }
       return stringBuilder.toString();
    }


    //------------------------------For Benchmarking----------------------------------------

    public  static void populateStorageService(){
        bulkInsertKeysWithoutTopic(files);
    }

    public static  void bulkInsertKeysWithoutTopic(File[] files) {
        for (File file : files) {
            System.out.println("File: " + file.getAbsolutePath());
            try {
                String content = readFile(file.getAbsolutePath(), Charset.defaultCharset());
                content = content.replace("\n", "").replace("\r", "");
                String[] commandArray={"put", file.getAbsolutePath(), content};
                String lineCommand = "put "+ file.getAbsolutePath()+ " "+  content;
                interpretCommand(commandArray,lineCommand);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static void bulkSubscribeToKeys (){
        for(File file: files){
            String[] commandArray={"subscribe", "-k", file.getAbsolutePath()};
            String lineCommand = "subscribe"+ " " + "-k" + " "+ file.getAbsolutePath();
            interpretCommand(commandArray,lineCommand);
        }
    }

    public static void subscribeToTopic (){

            String[] commandArray={"subscribe", "-t", "some_topic"};
            String lineCommand = "subscribe"+ " " + "-t" + " "+ "some_topic";
            interpretCommand(commandArray,lineCommand);

    }



    public static void bulkDeleteKeys() {
        for(File file: files){
            System.out.println("File: " + file.getAbsolutePath() .split("/")[8].replace(".", ""));
            String[] commandArray={"put",  file.getAbsolutePath() .split("/")[8].replace(".", "")};
            String lineCommand = "put" + " "+ file.getAbsolutePath() .split("/")[8].replace(".", "");
            interpretCommand(commandArray ,lineCommand);
        }
    }

    public static  void bulkInsertKeysWithTopic(File[] files) {
        for (File file : files) {
            System.out.println("File: " + file.getAbsolutePath());
            try {
                String content = readFile(file.getAbsolutePath(), Charset.defaultCharset());
                content = content.replace("\n", "").replace("\r", "");
                String[] commandArray={"put", "-t:some_topic", file.getAbsolutePath().split("/")[8].replace(".", ""), content};
                String lineCommand = "put "+  "-t:some_topic" +" " +file.getAbsolutePath().split("/")[8].replace(".", "")+ " "+  content;
                interpretCommand(commandArray,lineCommand);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
