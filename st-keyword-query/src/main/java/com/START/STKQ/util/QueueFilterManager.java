package com.START.STKQ.util;

import com.START.STKQ.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.github.nivdayan.FilterLibrary.filters.Filter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

class FilterWithKey {
    Filter filter;
    BytesKey bytesKey;

    public FilterWithKey(Filter filter, BytesKey bytesKey) {
        this.filter = filter;
        this.bytesKey = bytesKey;
    }
}

public class QueueFilterManager extends AbstractFilterManager {
    static Queue<FilterWithKey> queue;
    static Map<BytesKey, Filter> filters = new Hashtable<>();
    private static final int MAX_FILTER_COUNT = 512;
    private static int filterCount;
    private static Map<BytesKey, Long> count = new Hashtable<>();
    private static final ReentrantLock lock = new ReentrantLock();
    private static int reAllocateCount = 0;
    private static ChainedInfiniFilter filterForLoad = new ChainedInfiniFilter(3, 10);
    private static long time;

    public static Filter getFilter(BytesKey bytesKey) {
        long begin = System.nanoTime();
        Filter filter;
        long countForThisGrid = count.getOrDefault(bytesKey, -1L);
        if (countForThisGrid == -1) {
            return null;
        }
        lock.lock();
        try {
            filter = filters.get(bytesKey);
            if (filter == null) {
                try (FileInputStream fIn = new FileInputStream("/usr/data/bloom/dynamicBloom/all/" + bytesKey + ".txt")) {
                    if (++filterCount > MAX_FILTER_COUNT) {
                        reAllocate();
                    }
//                    filter = (ChainedInfiniFilter) oIn.readObject();
                    filter = filterForLoad.read(fIn);
                    filters.put(bytesKey, filter);
                    queue.add(new FilterWithKey(filter, bytesKey));
                }
            } else {
                return filters.get(bytesKey);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        time += System.nanoTime() - begin;
        return filter;
    }

    public static void reAllocate() {
        ++reAllocateCount;
        FilterWithKey filter = queue.poll();
        filters.remove(Objects.requireNonNull(filter).bytesKey);
    }

    public static void init() {
        queue = new LinkedList<>();
        String path = "/usr/data/count.txt";
        try(FileInputStream fIn = new FileInputStream(path);
            ObjectInputStream o = new ObjectInputStream(fIn);) {
            count = (Map<BytesKey, Long>) o.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getReAllocateCount() {
        return reAllocateCount;
    }

    public static long getTime() {
        return time;
    }
}
