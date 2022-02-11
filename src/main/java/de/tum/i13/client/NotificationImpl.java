package de.tum.i13.client;


import java.util.*;

public class NotificationImpl {


    private Map<String, List<Map<Long, String>>> keyChangeHistory;
    private Map<String, Map<String, List<Map<Long, String>>>> topicChangeHistory;

    public Map<String, List<Map<Long, String>>> getKeyChangeHistory() {
        return keyChangeHistory;
    }

    public Map<String, Map<String, List<Map<Long, String>>>> getTopicChangeHistory() {
        return topicChangeHistory;
    }

    public NotificationImpl() {
        keyChangeHistory = new LinkedHashMap<>();
        this.topicChangeHistory = new LinkedHashMap<>();
    }

    public void saveImpactedKeyValue(String key, String value) {
        if (keyChangeHistory.containsKey(key)) {
            Map<Long, String> map = new HashMap<>();
            map.put(System.currentTimeMillis(), value);
            keyChangeHistory.get(key).add(map);
        } else {
            List<Map<Long, String>> valueChanges = new ArrayList<>();
            Map<Long, String> map = new HashMap<>();
            map.put(System.currentTimeMillis(), value);
            valueChanges.add(map);
            keyChangeHistory.put(key, valueChanges);
        }


    }

    public void saveImpactedTopicKeyValue(String key, String topic, String value) {
        if (topicChangeHistory.containsKey(topic)) {
            if (topicChangeHistory.get(topic).containsKey(key)) {
                Map<Long, String> map = new HashMap<>();
                map.put(System.currentTimeMillis(), value);
                topicChangeHistory.get(topic).get(key).add(map);
            } else {
                List<Map<Long, String>> valueChanges = new ArrayList<>();
                Map<Long, String> map = new HashMap<>();
                map.put(System.currentTimeMillis(), value);
                valueChanges.add(map);
              
                topicChangeHistory.get(topic).put(key, valueChanges);
            }

        } else {
            topicChangeHistory.put(topic, new LinkedHashMap<String, List<Map<Long, String>>>() {{
                put(key, new ArrayList<Map<Long, String>>() {
                    {
                        Map<Long, String> map = new HashMap<>();
                        map.put(System.currentTimeMillis(), value);
                        add(map);
                    }
                });
            }});
        }
    }

}
