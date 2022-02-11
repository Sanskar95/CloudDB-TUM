package de.tum.i13.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by chris on 19.10.15.
 */
public class ActiveConnection implements AutoCloseable {
    private final Socket socket;
    private final PrintWriter output;
    private final BufferedReader input;

    public ActiveConnection(Socket socket, PrintWriter output, BufferedReader input) {
        this.socket = socket;

        this.output = output;
        this.input = input;
    }

    /**
     * Writes a strign to the server.
     * @param command : string to be written
     */
    public void write(String command) {
        output.write(command + "\r\n");
        output.flush();
    }

    /**
     * Attempts to read a message from the server
     * @return String containing the server response
     * @throws IOException If an I/O error occurs
     */
    public String readline() throws IOException {
        return input.readLine();
    }

    /**
     * attemps to close connection with the server
     * @throws Exception I/O error by closing the buffers or the socket connection
     */
    public void close() throws Exception {
        output.close();
        input.close();
        socket.close();
    }

    public String getInfo() {
        return "/" + this.socket.getRemoteSocketAddress().toString();
    }

    public int getPort() {
        if (!socket.isConnected()){
            return -1;
        }
        return socket.getPort();
    }

    public String getHostname(){
        if (!socket.isConnected()){
            return null;
        }
        return socket.getInetAddress().getHostName();
    }
}
