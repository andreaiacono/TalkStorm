package tophashtagsmap.utils;

import java.util.*;


public class MiscUtils {

    /**
     * returns the hashtags contained in the tweet
     *
     * @param tweet
     * @return the hastags; an empty Set if no hashtags are contained in the tweet
     */
    public static Set<String> getHashtags(String tweet) {

        Set<String> hashtags = new HashSet<>();
        int found = 1;
        int index = 0;

        while (found > 0) {
            found = tweet.indexOf("#", index);
            if (found > 0) {
                int end = tweet.indexOf(" ", found + 1);
                if (end == -1) {
                    end = tweet.length();
                }
                String hashtag = tweet.substring(found, end);
                index = end;
                hashtags.add(hashtag);
            }
        }
        return hashtags;
    }

    /**
     * returns a Map with the n top hashtags used
     *
     * @param hashtags
     * @param n
     * @return
     */
    public static Map<String, Long> getTopNHashtags(Map<String, Long> hashtags, int n) {

        Map<String, Long> sortedMap = sortByValue(hashtags);
        Map<String, Long> resultMap = new TreeMap<>();
        int counter = 0;
        for (String key : sortedMap.keySet()) {
            resultMap.put(key, sortedMap.get(key));
            if (++counter > n) break;
        }

        return resultMap;
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }


    public static <K, V> String mapToString(Map<K, V> map) {
        StringBuilder builder = new StringBuilder("{");
        for (K key : map.keySet()) {
            builder.append(key.toString()).append(": ").append(map.get(key).toString()).append(", ");
        }
        builder.deleteCharAt(builder.length()-2);
        return builder.append("}").toString();
    }
}
