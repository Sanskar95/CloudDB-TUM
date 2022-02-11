package de.tum.i13.server.cache.cacheDisplacementType;

import de.tum.i13.server.exception.NotAllowedException;
import de.tum.i13.server.filestorage.FileStorage;
import de.tum.i13.server.kv.KVStoreUtil;
import de.tum.i13.shared.MetaData;
import de.tum.i13.shared.ServerData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LRUCache  implements  CacheDisplacementType{
    Node head;
    Node tail;
    Map<String, Node> map;
    int capacity;
    private FileStorage fileStorage;
    public LRUCache(int capacity, FileStorage fileStorage) {
        head = new Node(String.valueOf(-1), String.valueOf(-1));
        tail = new Node(String.valueOf(-1), String.valueOf(-1));
        head.next = tail;
        tail.prev = head;
        map = new HashMap<>();
        this.capacity = capacity;
        this.fileStorage= fileStorage;
    }

    public String get(String key) {
        if (!map.containsKey(key)) {
            return null;
        }
        String val = map.get(key).val;
        moveToFront(map.get(key));
        return val;
    }

    @Override
    public void remove(String key) {
        map.remove(key);
    }

    @Override
    public void update(ServerData myServerData, MetaData metaData) {
        for (Map.Entry<String, Node> entry : map.entrySet()) {
            if (!KVStoreUtil.checkRange(myServerData, metaData, entry.getKey()))
                remove(entry.getKey());
        }
    }


    @Override
    public Integer currentSize() {
        return map.size();
    }

    public void put(String key, String value) throws NotAllowedException, IOException {
        if (!map.containsKey(key)) {
            if (map.size() == capacity) {
                evictKey(tail.prev);
                fileStorage.put(key, value, false);
                return;
            }
            Node node = new Node(key, value);
            map.put(key, node);
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
            node.prev = head;
        }
        else {
            map.get(key).val = value;
            moveToFront(map.get(key));
        }
    }

    private void moveToFront(Node node) {
        Node prev = node.prev;
        node.next.prev = prev;
        prev.next = node.next;
        node.next = head.next;
        head.next.prev = node;
        node.prev = head;
        head.next = node;
    }

    private void evictKey(Node node) {
        Node prev = node.prev;
        node.next.prev = prev;
        prev.next = node.next;
        map.remove(node.key);
    }

}



class Node {
    String key;
    String val;
    Node next;
    Node prev;

    public Node(String key, String val) {
        this.key = key;
        this.val = val;
        next = null;
        prev = null;
    }
}