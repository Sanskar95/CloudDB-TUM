package de.tum.i13.shared;

import java.io.IOException;
import java.net.ServerSocket;

public class Utils {

    public static String byteToHex(byte[] in) {
        StringBuilder sb = new StringBuilder();
        for(byte b : in) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    public static Integer getFreePort() throws IOException {

        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

}
