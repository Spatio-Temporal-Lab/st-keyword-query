package org.urbcomp.startdb.stkq.filter.manager;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.HashMap;
import java.util.Map;

public class BasicFilterManager implements IFilterManager {
    protected Map<BytesKey, IFilter> filters = new HashMap<>();

    private final int log2Size;
    private final int bitsPerKey;

    public BasicFilterManager(int log2Size, int bitsPerKey) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(index, filter);
        }
        return filter;
    }

    @Override
    public IFilter get(BytesKey index) {
        return filters.get(index);
    }

    @Override
    public IFilter getWithIO(BytesKey index) {
        IFilter filter = filters.get(index);
        if (filter == null) {
            filter = RedisIO.getFilter(0, index.getArray());
            if (filter != null) {
                filters.put(index, filter);
            }
            return filter;
        }
        return filter;
    }

    @Override
    public long ramUsage() {
        return filters.values().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    }

    @Override
    public void out() {
        RedisIO.putFilters(0, filters);
    }

    @Override
    public IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) {
        return getAndCreateIfNoExists(index);
    }
}
