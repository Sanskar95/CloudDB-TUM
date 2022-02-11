package de.tum.i13.ecs;

import de.tum.i13.shared.Utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class ServerCommunicationPortData {

   Map<String, CommunicationPort> communicationPortMap;

    public ServerCommunicationPortData() {
        this.communicationPortMap = new HashMap<>();
    }

    public void addServerData(String ip, String port, String communicationPort){
       communicationPortMap.put(hashServerHostIp(ip, port), new CommunicationPort(ip, communicationPort));
    }

    public void removeServerData(String ip, String port){
        communicationPortMap.remove(hashServerHostIp(ip, port));
    }


    public String hashServerHostIp(String ip, String port) {
        String stringToHash = new StringBuilder().append(ip).append(port).toString();
        return hashString(stringToHash);

    }
    public static String hashString(String stringToHash){
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(stringToHash.getBytes());
            return Utils.byteToHex(messageDigest);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }


    public  String getCommunicationPort(String ip , String port){
        return communicationPortMap.get(hashServerHostIp(ip, port)).getPort();
    }


}
