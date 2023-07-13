package com.START.STKQ.util;

import com.START.STKQ.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

class FilterWithHotness implements Comparable<FilterWithHotness> {
    Filter filter;
    long hotness;
    BytesKey bytesKey;

    public FilterWithHotness(Filter filter, BytesKey bytesKey, long hotness) {
        this.filter = filter;
        this.bytesKey = bytesKey;
        this.hotness = hotness;
    }

    public FilterWithHotness(Filter filter, BytesKey bytesKey) {
        this.filter = filter;
        this.bytesKey = bytesKey;
    }

    @Override
    public int compareTo(FilterWithHotness filterWithHotness) {
        return Long.compare(hotness, filterWithHotness.hotness);
    }

    @Override
    public String toString() {
        return String.valueOf(hotness);
    }
}

public class FilterManager extends AbstractFilterManager {
    private static final int MAX_FILTER_COUNT = 128;
    private static int filterCount;
    private static final Map<BytesKey, FilterWithHotness> filters = new Hashtable<>();
    private static Map<BytesKey, Long> count = new Hashtable<>();
    private static final Map<BytesKey, Long> hotnessMap = new Hashtable<>();
    private static final Multiset<FilterWithHotness> set = TreeMultiset.create();
    private static final ReentrantLock lock = new ReentrantLock();
    private static int reAllocateCount = 0;

    public static void init() {
        String path = "/usr/data/count.txt";
        try(FileInputStream fIn = new FileInputStream(path);
            ObjectInputStream o = new ObjectInputStream(fIn);) {
            count = (Map<BytesKey, Long>) o.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Filter getFilter(BytesKey bytesKey) {
        FilterWithHotness filter;
        long countForThisGrid = count.getOrDefault(bytesKey, -1L);
        if (countForThisGrid == -1) {
            return null;
        }
        lock.lock();
        try {
            filter = filters.get(bytesKey);
            if (filter == null) {
                try (FileInputStream fIn = new FileInputStream("/usr/data/bloom/dynamicBloom/all/" + bytesKey + ".txt");
                        ObjectInputStream oIn = new ObjectInputStream(fIn)) {
                    if (++filterCount > MAX_FILTER_COUNT) {
                        reAllocate();
                    }

                    hotnessMap.merge(bytesKey, countForThisGrid, Long::sum);
                    filter = new FilterWithHotness((Filter) oIn.readObject(), bytesKey, hotnessMap.get(bytesKey));

                    filters.put(bytesKey, filter);
                    set.add(filter);
                }
            } else {
                set.remove(filter);
                filter.hotness += countForThisGrid;
                hotnessMap.put(bytesKey, filter.hotness);
                set.add(filter);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return filter.filter;
    }

    public static void reAllocate() {
        for (FilterWithHotness filter : set) {
            set.remove(filter);
            filters.remove(filter.bytesKey);
            --filterCount;
            ++reAllocateCount;
            break;
        }
    }

    public void addCount(BytesKey bytesKey) {
        count.merge(bytesKey, 1L, Long::sum);
    }

    public static void showSize() {
        System.out.println(RamUsageEstimator.humanSizeOf(filters));
        System.out.println(RamUsageEstimator.humanSizeOf(set));
        System.out.println("filter count: " + filters.size());
        for (FilterWithHotness filter : filters.values()) {
            System.out.println(RamUsageEstimator.humanSizeOf(filter));
        }
    }

    public static int getReAllocateCount() {
        return reAllocateCount;
    }
}
