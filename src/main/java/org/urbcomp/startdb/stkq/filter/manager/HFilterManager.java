package org.urbcomp.startdb.stkq.filter.manager;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.*;

public class HFilterManager extends AbstractFilterManager {
    private int queryCount = 0;
    private long ramUsage = 0;
    private final long MAX_RAM_USAGE = 50 * 1024 * 1024;
    private static final int UPDATE_TIME = 10000;
    private static final int MAX_UPDATE_TIME = 50000;
    private final Map<BytesKey, FilterWithHotness> filters = new HashMap<>();
    private final TreeMap<Long, Set<IFilter>> sortedFilters = new TreeMap<>();

    public HFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(index, new FilterWithHotness(filter));
        }
        return filter;
    }

    public void build() {
        for (Map.Entry<BytesKey, FilterWithHotness> entry : filters.entrySet()) {
            FilterWithHotness filter = entry.getValue();

            long count = filter.getFilter().size();
            filter.setHotness(count);

            Set<IFilter> filterSet = sortedFilters.computeIfAbsent(count, k -> new HashSet<>());
            filterSet.add(filter.getFilter());
        }
    }

    public IFilter update(BytesKey index) {
        FilterWithHotness filter = filters.get(index);

        if (filter == null) {
            return null;
        }

        ++queryCount;

        long oldHotness = filter.getHotness();
        Set<IFilter> oldFilterSet = sortedFilters.get(oldHotness);
        oldFilterSet.remove(filter.getFilter());
        if (oldFilterSet.isEmpty()) {
            sortedFilters.remove(oldHotness);
        }

        long newHotness = filter.getFilter().size();
        filter.setHotness(newHotness);

        Set<IFilter> newFilterSet = sortedFilters.computeIfAbsent(newHotness, k -> new HashSet<>());
        newFilterSet.add(filter.getFilter());

        if (queryCount % UPDATE_TIME == 0 && queryCount <= MAX_UPDATE_TIME) {

            int size = filters.size();
            int upSize = getUpdateSize(size, queryCount / UPDATE_TIME);

            int i = 0;
            for (Map.Entry<Long, Set<IFilter>> filterEntry : sortedFilters.entrySet()) {
                boolean end = false;

                Set<IFilter> set = filterEntry.getValue();
                for (IFilter filter_ : set) {
                    if (++i <= upSize) {
                        long size1 = RamUsageEstimator.sizeOf(filter_);
                        filter_.sacrifice();
                        long size2 = RamUsageEstimator.sizeOf(filter_);
                        if (size2 > size1) {
                            System.err.println("error");
                        }
                    } else {
                        end = true;
                        break;
                    }
                }
                if (end) {
                    break;
                }
            }
        }

        return filter.getFilter();
    }

    public IFilter get(BytesKey index) {
        FilterWithHotness filterWithHotness = filters.get(index);
        return filterWithHotness == null ? null : filterWithHotness.getFilter();
    }

    public IFilter getWithIO(BytesKey index) {
        FilterWithHotness hFilter = filters.get(index);
        if (hFilter == null) {
            IFilter filter = RedisIO.getFilter(0, index.getArray());
            if (filter != null) {
                ramUsage += RamUsageEstimator.sizeOf(filter);
                if (ramUsage > MAX_RAM_USAGE) {
                    Iterator<Map.Entry<Long, Set<IFilter>>> iterator = sortedFilters.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, Set<IFilter>> entry = iterator.next();
                        Set<IFilter> filters = entry.getValue();
                        Iterator<IFilter> setIterator = filters.iterator();
                        while (setIterator.hasNext()) {
                            IFilter filterToRemove = setIterator.next();
                            setIterator.remove();
                            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
                            if (ramUsage < MAX_RAM_USAGE) {
                                break;
                            }
                        }
                        if (filters.isEmpty()) {
                            iterator.remove();
                        }
                        if (ramUsage < MAX_RAM_USAGE) {
                            break;
                        }
                    }
                }
                filters.put(index, new FilterWithHotness(filter));
            }
            return filter;
        }
        return hFilter.getFilter();
    }

    public long size() {
        System.out.println("filter count: " + filters.size());
        System.out.println("map size: " + RamUsageEstimator.sizeOf(filters));
        System.out.println("sorted filter size: " + RamUsageEstimator.sizeOf(sortedFilters));
        long size = 0;
        for (Map.Entry<BytesKey, FilterWithHotness> filter : filters.entrySet()) {
            size += RamUsageEstimator.sizeOf(filter.getValue().getFilter());
        }
        return size;
    }

    public void out() {
        String tableName = "HFilters";
        RedisIO.putFiltersWithHotness(tableName, filters);
    }

    public void compress() {
        Iterator<Map.Entry<BytesKey, FilterWithHotness>> iterator = filters.entrySet().iterator();
        long sizeNow = size();
        System.out.println("size now: " + sizeNow);
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, FilterWithHotness> entry = iterator.next();
            sizeNow -= RamUsageEstimator.sizeOf(entry.getValue().getFilter());
            iterator.remove();
            if (sizeNow <= 792433880) {
                break;
            } else {
                System.out.println("size after delete: " + sizeNow);
            }
        }
    }
}
