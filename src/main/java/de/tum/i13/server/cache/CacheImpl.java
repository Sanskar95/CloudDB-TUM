package de.tum.i13.server.cache;

import de.tum.i13.server.cache.cacheDisplacementType.CacheDisplacementType;
import de.tum.i13.server.cache.cacheDisplacementType.FIFOCache;
import de.tum.i13.server.cache.cacheDisplacementType.LFUCache;
import de.tum.i13.server.cache.cacheDisplacementType.LRUCache;
import de.tum.i13.server.exception.KeyNotFoundException;
import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.IOException;
import java.util.Objects;

public class CacheImpl implements Cache{

    //    private ConcurrentHashMap<String, String> cacheStorage;
    private CacheDisplacementType cacheDisplacementType;



    private CacheDisplacementType getCacheDisplacementType(Integer cacheSize, String cacheDisplacementStrategy, FileStorage fileStorage){

        if(cacheDisplacementStrategy.equals("FIFO")){
            return  new FIFOCache(cacheSize, fileStorage);
        }else if(cacheDisplacementStrategy.equals("LFU")){
            return new LFUCache(cacheSize, fileStorage);
        } else if(cacheDisplacementStrategy.equals("LRU")){
            return new LRUCache(cacheSize, fileStorage);
        }
        return new FIFOCache(cacheSize, fileStorage);
    }

    public CacheImpl(Integer cacheSize, String cacheDisplacementStrategy, FileStorage fileStorage) {
        //        this.cacheStorage = cacheStorage;
        this.cacheDisplacementType= getCacheDisplacementType(cacheSize, cacheDisplacementStrategy, fileStorage);
    }

    @Override
    public void put(String key, String value) throws NotAllowedException, IOException {
        cacheDisplacementType.put(key, value);
    }

    @Override
    public String get(String key) throws KeyNotFoundException {
        return cacheDisplacementType.get(key);
    }

    @Override
    public void remove(String key) throws KeyNotFoundException {
        if (Objects.isNull(cacheDisplacementType.get(key))) {
            return;
        } else{
            cacheDisplacementType.remove(key);
        }
    }

    @Override
    public void update(ServerData myServerData, MetaData metaData) {
        cacheDisplacementType.update(myServerData, metaData);
    }


    public Integer currentCacheSize (){
        return cacheDisplacementType.currentSize();
    }
}
