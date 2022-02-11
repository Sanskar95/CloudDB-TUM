package de.tum.i13.server.filestorage;

import de.tum.i13.server.exception.KeyNotFoundException;
import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreUtil;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public class FileStorage {
    private File fileName;
    private File fileForReplication;
    public static Logger logger = Logger.getLogger(FileStorage.class.getName());

    public FileStorage(Path path) {
        logger.setLevel(StartSimpleNioServer.loggerLevel);
        Path partialPath = Paths.get("storage.txt");
        Path resolvedPath = path.resolve(partialPath);
        this.fileName = resolvedPath.toFile();
        Path partialPathForReplication = Paths.get("replication.txt");
        Path resolvedPathForReplication= path.resolve(partialPathForReplication);
        this.fileForReplication= resolvedPathForReplication.toFile();
        try {
            fileForReplication.createNewFile();
            fileName.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * This function put the element with key and value in the file "storage.txt"
     *
     * @param key   the key that we are putting.
     * @param value the value corresponding to the key.
     * @throws IOException
     */
    public void put(String key, String value, Boolean replication) throws IOException, NotAllowedException {
        File thisFile= (replication) ? fileForReplication : fileName;
        FileWriter fileWriter = null;
        FileReader fileReader = null;
        try {
            Properties prop = new Properties();
            if (thisFile.exists()) {
                fileReader = new FileReader(thisFile);
                prop.load(fileReader);

            }
            value.replace("=","\\=");
            value.replace(":","\\:");
            prop.setProperty(key, value);
            fileWriter = new FileWriter(thisFile);
            prop.store(fileWriter, null);
        } catch (IOException e) {
            e.printStackTrace();
            if (e instanceof IOException)
                throw new IOException();
        } finally {
            if (!Objects.isNull(fileWriter)) {
                fileWriter.close();
            }
            if (!Objects.isNull(fileReader)) {
                fileReader.close();
            }


        }
    }

    /**
     * This function return the element with key from the file "storage.txt"
     *
     * @param key the key that we are searching for
     * @return The value to which the key is mapped.
     * @throws IOException
     * @throws KeyNotFoundException
     */
    public String get(String key) throws IOException, KeyNotFoundException {

        try (FileReader fileReader = new FileReader(fileName); FileReader fileReaderRepli= new FileReader(fileForReplication)) {
            Properties prop = new Properties();
            prop.load(fileReader);
            prop.load(fileReaderRepli);
            if (Objects.isNull(prop.getProperty(key))) {
                throw new KeyNotFoundException("Key Not Found");
            }
            String value =prop.getProperty(key);
            value.replace("\\:",":");
            value.replace("\\=","=");
            return value;
        } catch (IOException | KeyNotFoundException e) {
            e.printStackTrace();
            if (e instanceof IOException)
                throw new IOException();
            throw new KeyNotFoundException("This is not Allowed");
        }
    }


    /**
     * This function remove the element with key=key from the file "storage.txt"
     *
     * @param key the key that we are searching for
     * @throws IOException
     * @throws NotAllowedException
     */

    public void remove(String key, Boolean replication) throws IOException, NotAllowedException {
        File thisFile= (replication) ? fileForReplication : fileName;
        FileReader fileReader = null;
        FileWriter fileWriter = null;
        try {
            fileReader = new FileReader(thisFile);
            Properties prop = new Properties();
            prop.load(fileReader);
            fileWriter = new FileWriter(thisFile);
            if (Objects.isNull(prop.getProperty(key))) {
                throw new NotAllowedException("This is not Allowed");
            } else {
                prop.remove(key);
                prop.store(fileWriter, null);
            }
        } catch (IOException | NotAllowedException e) {
            e.printStackTrace();
            if (e instanceof IOException)
                throw new IOException();
            throw new NotAllowedException("This is not Allowed as IO");
        } finally {
            fileWriter.close();
            fileReader.close();
        }
    }


    /**
     * This function update the file storage after receiving the metadata
     * @param myServerData the serverData of this server
     * @param metaData the metaData
     */
    public void update(ServerData myServerData, MetaData metaData){
        try (FileReader fileReader = new FileReader(fileName)) {
            Properties prop = new Properties();
            prop.load(fileReader);

            Set<String> keys = prop.stringPropertyNames();
            for (String key : keys) {
                if (!KVStoreUtil.checkRange(myServerData, metaData, key))
                    remove(key,false);
            }
        } catch (Exception e ) {
            e.printStackTrace();
          return;
        }
    }

    /**
     * This function retrieves the data that need to be handed off.
     * @param index the endIndex of the server receiving the data
     * @param myServerData ther serverData of this server
     * @return
     */

    public String getWithRange(String index, ServerData myServerData) {
        try (FileReader fileReader = new FileReader(fileName)) {
            String keyValues = "nr ";
            Properties prop = new Properties();
            prop.load(fileReader);
            String keyValuesReplication = "r ";

            Set<String> keys = prop.stringPropertyNames();
            for (String key : keys) {
                if (index.equals(Constants.SEND_ALL_KEYS)){
                    keyValues += key + " " + prop.getProperty(key) + " ";
                }else if(index.equals((Constants.YOUR_RANGE))){
                    keyValuesReplication+= key +" "+ prop.getProperty(key) + " ";
                }
                else if ((index.compareTo(myServerData.getEndIndex())>0 && MetaData.hashString(key).compareTo(myServerData.getEndIndex())>0 && MetaData.hashString(key).compareTo(index)<0))
                    //when index is the send_all_keys constants we add all keys
                    keyValuesReplication += key + " " + prop.getProperty(key) + " ";
                else if((myServerData.getStartIndex() == myServerData.getEndIndex()) && (index.compareTo(myServerData.getEndIndex())<0 && (MetaData.hashString(key).compareTo(index)<0 || MetaData.hashString(key).compareTo(myServerData.getEndIndex())>0)))
                    keyValues += key + " " + prop.getProperty(key) + " ";
                else if(index.compareTo(myServerData.getEndIndex())<0 && (MetaData.hashString(key).compareTo(index)<0 && MetaData.hashString(key).compareTo(myServerData.getStartIndex())>0))
                    //when index is the send_all_keys constants we add all keys
                    keyValues += key + " " + prop.getProperty(key) + " ";
            }
            if(index.equals(Constants.YOUR_RANGE))
                return keyValuesReplication;
            return keyValues;
        } catch (FileNotFoundException fileNotFoundException ){
            logger.warning("file not found ! handoff will not hand any key");
            return "";
        }
        catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }




    /**
     * This function remove the data with range.
     * @param commands the startindex of the server receiving the data
     * @param myServerData ther serverData of this server
     * @return
     */

    public String removeWithRange(List<String> commands, ServerData myServerData) {
        try (FileReader fileReader = new FileReader(fileForReplication)) {
            Properties prop = new Properties();
            prop.load(fileReader);

            Set<String> keys = prop.stringPropertyNames();
            if (commands.get(1).equals(Constants.ONLY_MY_RANGE)) {
                for (String key : keys) {
                        remove(key,true);
                }
            }else{
                if(commands.get(3).compareTo(commands.get(2))<0){
                    for(String key: keys){
                        if( (MetaData.hashString(key).compareTo(commands.get(1))>0 && MetaData.hashString(key).compareTo(Constants.HEX_START_INDEX)>0) || (MetaData.hashString(key).compareTo(Constants.HEX_START_INDEX)>0 && MetaData.hashString(key).compareTo(commands.get(2))<0))
                        remove(key,true);
                    }}else{
                    for(String key: keys){
                        if(MetaData.hashString(key).compareTo(commands.get(1))>0 && MetaData.hashString(key).compareTo(commands.get(2))<0)
                        remove(key,true);
                    }
                }
            }
            return "";

        } catch (FileNotFoundException fileNotFoundException ){
            logger.warning("file not found ! handoff will not hand any key");
            return "";
        }
        catch (IOException e) {
            e.printStackTrace();
            return "";
        }
        catch (NotAllowedException e) {
            e.printStackTrace();
            return "";
        }
    }


}

