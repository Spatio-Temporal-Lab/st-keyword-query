package org.urbcomp.startdb.stkq.util;

import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.*;

public class MapUtil {
    public static <K, V extends Comparable<? super V>> List<K> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());

        List<K> result = new ArrayList<>();
        for (Map.Entry<K, V> entry : list) {
            result.add(entry.getKey());
        }

        return result;
    }

    public static List<BytesKey> sortByTime(Map<BytesKey, Integer> map) {
        List<Map.Entry<BytesKey, Integer>> list = new ArrayList<>(map.entrySet());
        list.sort(Comparator.comparingInt(e -> ByteUtil.toInt(Arrays.copyOfRange(e.getKey().getArray(), 4, 7))));

        List<BytesKey> result = new ArrayList<>();
        for (Map.Entry<BytesKey, Integer> entry : list) {
            result.add(entry.getKey());
        }

        return result;
    }
}
