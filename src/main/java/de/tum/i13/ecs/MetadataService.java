package de.tum.i13.ecs;

import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

public class MetadataService {

    private MetaData metaData;


    private Object mutex = new Object();

    public MetadataService() {
        this.metaData = new MetaData();
    }

    public ServerData addServer(String ip, String port){
        synchronized (mutex){
            return  metaData.addServer(ip, port);
        }

    }

    public ServerData removeServer(String ip, String port){
        synchronized (mutex){
            return metaData.removeServer(ip, port);
        }
    }


    public MetaData getMetaData(){
        return metaData;
    }
}
