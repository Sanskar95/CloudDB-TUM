package de.tum.i13.server.cache.cacheDisplacementType;

import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.server.kv.KVStoreUtil;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.IOException;
import java.util.*;

public class LFUCache implements CacheDisplacementType {
    Map<String, String> keyValueMap = new HashMap<>();
    Map<String, Integer> keyFrequencyMap = new HashMap<>();
    Map<Integer, Set<String>> frequencyMap = new HashMap<>();
    FileStorage fileStorage;
    int cap;
    int min = -1;

    public LFUCache(int capacity, FileStorage fileStorage) {
        cap = capacity;
        frequencyMap.put(1, new LinkedHashSet<>());
        this.fileStorage = fileStorage;
    }

    public String get(String key) {
        if (!keyValueMap.containsKey(key))
            return null;
        // Get the current key's freq
        int freq = keyFrequencyMap.get(key);

        // Update current key's freq
        keyFrequencyMap.put(key, freq + 1);
        frequencyMap.get(freq).remove(key);

        // Update the min freq
        if (freq == min && frequencyMap.get(freq).size() == 0)
            min++;

        // Update current key's freq
        if (!frequencyMap.containsKey(freq + 1))
            frequencyMap.put(freq + 1, new LinkedHashSet<>());
        frequencyMap.get(freq + 1).add(key);

        return keyValueMap.get(key);
    }

    @Override
    public void remove(String key) {
        keyValueMap.remove(key);
        keyFrequencyMap.remove(key);
//        removeFromFreqMap(key, freqMap);

    }

    @Override
    public Integer currentSize() {
        return keyValueMap.size();
    }

    public void put(String key, String value) throws NotAllowedException, IOException {
        // Base case
        if (cap == 0)
            return;

        // Update value
        if (keyValueMap.containsKey(key)) {
            keyValueMap.put(key, value);
            get(key);// update the freq for the current key
        } else {
            // Check if exceed the capacity
            if (keyValueMap.size() == cap) {
                Set<String> curlist = frequencyMap.get(min);
                String top = poll(curlist);
                keyValueMap.remove(top);// remove lfu
                fileStorage.put(key,value, false);
            }
            keyValueMap.put(key, value);
            keyFrequencyMap.put(key, 1);
            min = 1;// because we just add a new element
            frequencyMap.get(1).add(key);
        }
    }

    private String poll(Set<String> set) {
        Iterator<String> iterator = set.iterator();
        String top = iterator.next();
        set.remove(top);
        return top;
    }

//    private String removeFromFreqMap(String key,Map<Integer, Set<String>> freqMap ) {
//        Set<String> curlist = freqMap.get(key);
//        Iterator<String> iterator = curlist.iterator();
//        while (!iterator.next().equals(key)){
//           if(iterator.next().equals(key)){
//               curlist.remove(key);
//               f
//           }
//        }
//        String top = iterator.next();
//        set.remove(top);
//        return top;
//    }

    @Override
    public void update(ServerData myServerData, MetaData metaData) {
        for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
            if (!KVStoreUtil.checkRange(myServerData, metaData, entry.getKey()))
                remove(entry.getKey());
        }
    }


}
