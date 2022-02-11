package de.tum.i13.server.cache.cacheDisplacementType;

import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.server.kv.KVStoreUtil;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FIFOCache implements CacheDisplacementType {

    private Integer cacheSize;
    private ConcurrentHashMap<String, String> storage = new ConcurrentHashMap<>();
    private FileStorage fileStorage;

    public FIFOCache(Integer cacheSize, FileStorage fileStorage) {
        this.cacheSize = cacheSize;
        this.fileStorage = fileStorage;
    }

    @Override
    public void put(String key, String value) throws NotAllowedException, IOException {
        if (storage.containsKey(key)) {
            storage.replace(key, value);
            return;
        } else if (storage.size() == cacheSize) {

            storage.remove(key);
            fileStorage.put(key, value, false);
            return;
        }
        storage.put(key, value);
    }

    @Override
    public String get(String key) {
        if (!storage.containsKey(key))
            return null;
        return storage.get(key);
    }

    @Override
    public void remove(String key) {
        storage.remove(key);
    }

    @Override
    public Integer currentSize() {
        return storage.size();
    }

    @Override
    public void update(ServerData myServerData, MetaData metaData) {
        for (Map.Entry<String, String> entry : storage.entrySet()) {
            if (!KVStoreUtil.checkRange(myServerData, metaData, entry.getKey()))
                remove(entry.getKey());
        }

    }

}


