package com.START.STKQ.util.FilterManager;

import com.START.STKQ.constant.Constant;
import com.START.STKQ.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
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
    private static final int MAX_FILTER_COUNT = 512;
    private static int filterCount;
    private static final Map<BytesKey, FilterWithHotness> filters = new Hashtable<>();
    private static Map<BytesKey, Long> count = new Hashtable<>();
    private static final Map<BytesKey, Long> hotnessMap = new Hashtable<>();
    private static final Multiset<FilterWithHotness> set = TreeMultiset.create();
    private static final ReentrantLock lock = new ReentrantLock();
    private static int reAllocateCount = 0;
    private static final ChainedInfiniFilter filterForLoad = new ChainedInfiniFilter(3, 10);
    private static final String DIR_NAME = "/usr/data/bloom/dynamicBloom/all" + Constant.S_FILTER_ITEM_LEVEL + Constant.T_FILTER_ITEM_LEVEL + "/";
    private static long time;

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
        long begin = System.nanoTime();
        FilterWithHotness filter;
        long countForThisGrid = count.getOrDefault(bytesKey, -1L);
        if (countForThisGrid == -1) {
            return null;
        }
        lock.lock();
        try {
            filter = filters.get(bytesKey);
            if (filter == null) {
                try (FileInputStream fIn = new FileInputStream(DIR_NAME + bytesKey + ".txt")) {
                    if (++filterCount > MAX_FILTER_COUNT) {
                        reAllocate();
                    }

                    hotnessMap.merge(bytesKey, countForThisGrid, Long::sum);

                    ChainedInfiniFilter cFilter = filterForLoad.read(fIn);
//                    System.out.println(cFilter == null);
                    filter = new FilterWithHotness(cFilter, bytesKey, hotnessMap.get(bytesKey));

                    filters.put(bytesKey, filter);
                    set.add(filter);
                }
            } else {
                set.remove(filter);
                filter.hotness += countForThisGrid;
                hotnessMap.put(bytesKey, filter.hotness);
                set.add(filter);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        time += System.nanoTime() - begin;
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

    public static long getTime() {
        return time;
    }
}
