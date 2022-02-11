package de.tum.i13.server.kv;

import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;
import java.util.logging.Logger;

public class KVStoreUtil {

    public static Logger logger = Logger.getLogger(KVStoreUtil.class.getName());

    /**
     * This function is used to check if the key is in the range of the server
     * @param key the used key
     * @return true if the key is in my range
     */
    public static Boolean checkRange(ServerData myServerData, MetaData metaData, String key){
        //ServerData myServerData= metaData.getServerFromIdentifiers(ecsHost,String.valueOf(myport));

        String keyHash = MetaData.hashString(key);
        logger.info("checking range for key : " + key + " and hash " + keyHash);
        boolean isResponsible;
        if (myServerData.getStartIndex().compareTo(myServerData.getEndIndex()) >= 0){
            isResponsible = keyHash.compareTo(myServerData.getStartIndex()) >= 0 || keyHash.compareTo(myServerData.getEndIndex()) <= 0;
        } else{
            isResponsible = keyHash.compareTo(myServerData.getStartIndex()) >= 0 && keyHash.compareTo(myServerData.getEndIndex()) <=0;
        }


        logger.info("This server " + myServerData.getIp() + ":" + myServerData.getPort() + " is responsible for it ? " + isResponsible);
        return isResponsible;

    }

    public static String[] getRangeWithDuplication(String myIp, String myPort, MetaData metaData){
        ServerData myServerData = metaData.getServerFromIdentifiers(myIp,myPort);
        if(metaData.getServerDataMap().size()<3){

            return new String[]{myServerData.getStartIndex(), myServerData.getEndIndex()};
        }else{
            String startIndex= metaData.getServerNextPredecessor(metaData.hashServerHostIp(myIp,myPort)).getStartIndex();
            String endIndex=myServerData.getEndIndex();
            return  new String[]{startIndex,endIndex};
        }

    }


    /**
     * This function is used to check if the key is in the range of the server
     * @param key the used key
     * @return true if the key is in my range
     */
    public static Boolean checkRangeWithDuplication(String myIp,String myPort, MetaData metaData, String key){
        if(metaData.getServerDataMap().size()==3)
            return true;
        String[] indexes= getRangeWithDuplication(myIp,myPort,metaData);
        String startIndex=indexes[0];
        String endIndex=indexes[1];
        String keyHash = MetaData.hashString(key);
        logger.info("checking range for key : " + key + " and hash " + keyHash);
        boolean isResponsible;
        if (startIndex.compareTo(endIndex) >= 0){
            isResponsible = keyHash.compareTo(startIndex) >= 0 || keyHash.compareTo(endIndex) <= 0;
        } else{
            isResponsible = keyHash.compareTo(startIndex) >= 0 && keyHash.compareTo(endIndex) <=0;
        }


        logger.info("This server " + myIp + ":" + myPort + " is responsible for it ? " + isResponsible);
        return isResponsible;

    }

}
