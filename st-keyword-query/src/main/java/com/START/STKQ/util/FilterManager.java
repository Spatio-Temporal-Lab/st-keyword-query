package com.START.STKQ.util;

import com.START.STKQ.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.collect.TreeMultiset;
import com.google.common.collect.Multiset;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

class FilterWithHotness implements Comparable<FilterWithHotness> {
    Filter filter;
    long hotness;
    BytesKey bytesKey;

    FilterWithHotness(Filter filter, BytesKey bytesKey, long hotness) {
        this.filter = filter;
        this.bytesKey = bytesKey;
        this.hotness = hotness;
    }

    FilterWithHotness(Filter filter, BytesKey bytesKey) {
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

public class FilterManager {
    // assume that we max use ram for 128MB
    private static final int MAX_RAM_USAGE = 128 * 1024;
    private static int totalSize;
    private static final Map<BytesKey, FilterWithHotness> filters = new Hashtable<>();
    private static Map<BytesKey, Long> count = new Hashtable<>();
    private static final Multiset<FilterWithHotness> set = TreeMultiset.create();
    private static final ReentrantLock lock = new ReentrantLock();

    public static void loadCount() {
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
        lock.lock();
        try {
            filter = filters.get(bytesKey);
            long countForThisGrid = count.getOrDefault(bytesKey, 0L);
            if (countForThisGrid == 0) {
                return null;
            }
            if (filter == null) {
                try (FileInputStream fIn = new FileInputStream("/usr/data/bloom/dynamicBloom/all/" + bytesKey + ".txt");
                        ObjectInputStream oIn = new ObjectInputStream(fIn)) {
                    filter = new FilterWithHotness((Filter) oIn.readObject(), bytesKey);

                    filters.put(bytesKey, filter);
                    set.add(filter);

                    long size = RamUsageEstimator.shallowSizeOf(filter);
                    totalSize += size;
                    if (totalSize > MAX_RAM_USAGE) {
                        reAllocate();
                    }
                }
            } else {
                set.remove(filter);
                filter.hotness += countForThisGrid;
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
            break;
        }
    }

    public void addCount(BytesKey bytesKey) {
        count.merge(bytesKey, 1L, Long::sum);
    }

}
