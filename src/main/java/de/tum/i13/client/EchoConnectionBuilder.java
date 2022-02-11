package de.tum.i13.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by chris on 19.10.15.
 */
public class EchoConnectionBuilder {

    private final String host;
    private final int port;

    public EchoConnectionBuilder(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the
     * @return
     * @throws java.net.UnknownHostException : if the port is wrong
     * @throws IllegalArgumentException :
     * @throws IOException :
     */
    public ActiveConnection connect() throws java.net.UnknownHostException, IllegalArgumentException, IOException  {
        Socket s = new Socket(this.host, this.port);

        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));

        return new ActiveConnection(s, output, input);
    }
}
