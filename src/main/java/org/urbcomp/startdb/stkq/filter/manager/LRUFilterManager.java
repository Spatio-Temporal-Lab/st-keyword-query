package org.urbcomp.startdb.stkq.filter.manager;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.*;

public class LRUFilterManager extends AbstractFilterManager {
    private int queryCount = 0;
    private long ramUsage = 0;
    private final long MAX_RAM_USAGE = 50 * 1024 * 1024;
    private static final int UPDATE_TIME = 10000;
    private static final int MAX_UPDATE_TIME = 50000;
    protected Map<BytesKey, IFilter> filters = new LinkedHashMap<>(100_0000, .75F, true);

    public LRUFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(index, filter);
        }
        return filter;
    }

    public IFilter get(BytesKey index) {
        return filters.get(index);
    }

    public IFilter getWithIO(BytesKey index) {
        IFilter filter = filters.get(index);
        if (filter == null) {
            filter = RedisIO.getFilter(0, index.getArray());
            if (filter != null) {
                filters.put(index, filter);
                ramUsage += RamUsageEstimator.sizeOf(filter);
                if (ramUsage > MAX_RAM_USAGE) {
                    Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<BytesKey, IFilter> entry = iterator.next();
                        IFilter filterToRemove = entry.getValue();
                        ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
                        iterator.remove();
                        if (ramUsage < MAX_RAM_USAGE) {
                            break;
                        }
                    }
                }
            }
            return filter;
        }
        return filter;
    }

    public int getUpdateSize(int size, int k) {
        return size - k * 50000;
    }

    public IFilter update(BytesKey index) {
        IFilter filter = filters.get(index);

        if (filter == null) {
            return null;
        }

        ++queryCount;


        if (queryCount % UPDATE_TIME == 0 && queryCount <= MAX_UPDATE_TIME) {

            int size = filters.size();
            int upSize = getUpdateSize(size, queryCount / UPDATE_TIME);

            int i = 0;
            for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
                IFilter filter_ = entry.getValue();
                if (++i <= upSize) {
                    long size1 = RamUsageEstimator.sizeOf(filter_);
                    filter_.sacrifice();
                    long size2 = RamUsageEstimator.sizeOf(filter_);
                    if (size2 > size1) {
                        System.err.println("error");
                    }
                } else {
                    break;
                }
            }
        }

        return filter;
    }

    public IFilter getAndUpdate(BytesKey index) {
        return update(index);
    }

    public long size() {
        long size = 0;
        for (Map.Entry<BytesKey, IFilter> filterEntry : filters.entrySet()) {
            size += RamUsageEstimator.sizeOf(filterEntry.getValue());
        }
        return size;
    }

    public void compress() {
        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();
        long sizeNow = size();
        System.out.println("size now: " + sizeNow);
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();
            sizeNow -= RamUsageEstimator.sizeOf(entry.getValue());
            iterator.remove();
            if (sizeNow <= 792433880) {
                break;
            } else {
                System.out.println("size after delete: " + sizeNow);
            }
        }
    }

    public void out() throws IOException {
        String tableName = "lruFilters";
        System.out.println("lru filters put: ");
        RedisIO.putLRUFilters(tableName, filters);
    }
}
