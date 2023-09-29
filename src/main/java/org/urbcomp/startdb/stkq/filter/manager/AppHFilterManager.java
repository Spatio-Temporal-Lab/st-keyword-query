package org.urbcomp.startdb.stkq.filter.manager;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.*;

class FilterWithIdx {
    final IFilter filter;
    int idx;

    public FilterWithIdx(IFilter filter, int idx) {
        this.filter = filter;
        this.idx = idx;
    }

    public IFilter getFilter() {
        return filter;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }
    @Override
    public String toString() {
        return "FilterWithIdx{" +
                "filter=" + filter +
                ", idx=" + idx +
                '}';
    }
}

public class AppHFilterManager extends AbstractFilterManager {
    private int queryCount = 0;
    private static final int UPDATE_TIME = 10000;
    private static final int MAX_UPDATE_TIME = 50000;
    private final Map<BytesKey, FilterWithIdx> filters = new HashMap<>();
    private final List<FilterWithIdx> sortedFilters = new ArrayList<>();

    public AppHFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            FilterWithIdx filterWithIdx = new FilterWithIdx(filter, sortedFilters.size() +  1);
            sortedFilters.add(filterWithIdx);
            filters.put(index, filterWithIdx);
        }
        return filter;
    }

    public void build() {
        sortedFilters.sort(Comparator.comparingInt(o -> o.filter.appSize()));
        int n = sortedFilters.size();
        for (int i = 0; i < n; ++i) {
            FilterWithIdx filterWithIdx = sortedFilters.get(i);
            filterWithIdx.setIdx(i + 1);
        }
    }

    public int getUpdateSize(int size) {
        return (int) (size * 0.74);
    }

    public void moveBackward(int idx, int step) {
        int maxIdx = Math.min(filters.size() - 1, idx + step);
        FilterWithIdx temp = sortedFilters.get(idx);
        temp.setIdx(maxIdx);
        for (int i = idx; i < maxIdx; ++i) {
            sortedFilters.get(i + 1).setIdx(i);
            sortedFilters.set(i, sortedFilters.get(i + 1));
        }
        sortedFilters.set(maxIdx, temp);
    }

    public IFilter update(BytesKey index) {
        FilterWithIdx filterWithIdx = filters.get(index);
        if (filterWithIdx == null) {
            return null;
        }

        ++queryCount;
        moveBackward(filterWithIdx.idx, filterWithIdx.filter.appSize());

        if (queryCount % UPDATE_TIME == 0 && queryCount <= MAX_UPDATE_TIME) {
            int size = filters.size();
            int upSize = getUpdateSize(size);
            for (int i = 0; i < upSize; ++i) {
                IFilter filter_ = sortedFilters.get(i).filter;
                long size1 = RamUsageEstimator.sizeOf(filter_);
                filter_.sacrifice();
                long size2 = RamUsageEstimator.sizeOf(filter_);
                if (size2 > size1) {
                    System.err.println("error");
                }
            }
        }

        return filterWithIdx.filter;
    }

    public IFilter get(BytesKey index) {
        FilterWithIdx filterWithIdx = filters.get(index);
        return filterWithIdx == null ? null : filterWithIdx.filter;
    }

    public IFilter getAndUpdate(BytesKey index) {
        return update(index);
    }

    public List<FilterWithIdx> getFilters() {
        return sortedFilters;
    }

    public long size() {
        System.out.println("filter count: " + filters.size());
        long size = 0;
        for (FilterWithIdx filter : sortedFilters) {
            size += RamUsageEstimator.sizeOf(filter.filter);
        }
        return size;
    }
}
