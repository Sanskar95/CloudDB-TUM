package de.tum.i13.eventpublisher;

import de.tum.i13.ecs.CommunicationPort;

import java.util.Objects;

public class SubscriberData {
    private String port;
    private String ip;
    private boolean isTopic; //to know if this is a topic (true) or key (false) subscription

    public SubscriberData(String ip, String port, boolean topic) {
        this.port = port;
        this.ip = ip;
        this.isTopic = topic;
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

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean topic) {
        isTopic = topic;
    }
}
