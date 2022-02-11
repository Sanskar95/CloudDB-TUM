package de.tum.i13.shared;

public class ServerData {
    private int port;
    private String ip;
    private String startIndex;
    private String endIndex;

    public ServerData(int port, String ip, String startIndex, String endIndex) {
        this.port = port;
        this.ip = ip;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(String startIndex) {
        this.startIndex = startIndex;
    }


    public String getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(String endIndex) {
        this.endIndex = endIndex;
    }
}
