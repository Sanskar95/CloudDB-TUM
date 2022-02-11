package de.tum.i13.server.cache.cacheDisplacementType;

import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.IOException;

public interface CacheDisplacementType {

   /**
    * This function updates the element with key=key from the hashmap.
    *
    * @param key the key that we are searching for
    */
   void put(String key, String value) throws NotAllowedException, IOException;

   /**
    * This function return the element with key=key from the hashmap.
    *
    * @param key the key that we are searching for
    * @return The value to which the key is mapped, null if the key is not present in
    * the concurrentHashMap
    */

   String get (String key);

   /**
    * This function remove the element with key=key from the concurrentHashMap.
    *
    * @param key the key that we are searching for
    */

   void remove (String key);

   /**
    * This function return the current size of cache
    */

   Integer currentSize();

   void update(ServerData myServerData, MetaData metaData);

}
