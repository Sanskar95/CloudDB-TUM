package de.tum.i13.ecs;

import java.util.Objects;

public class CommunicationPort {
    String port;
    String ip;

    public CommunicationPort(String ip, String port) {
        this.port = port;
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommunicationPort)) return false;
        CommunicationPort that = (CommunicationPort) o;
        return Objects.equals(port, that.port) && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(port, ip);
    }
}
