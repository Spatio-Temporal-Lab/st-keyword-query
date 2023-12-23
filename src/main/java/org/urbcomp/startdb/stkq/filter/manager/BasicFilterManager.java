package org.urbcomp.startdb.stkq.filter.manager;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.HashMap;
import java.util.Map;

public class BasicFilterManager extends AbstractFilterManager {
    protected Map<BytesKey, IFilter> filters = new HashMap<>();

    public BasicFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey, null, 0);
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
            }
            return filter;
        }
        return filter;
    }

    @Override
    public long ramUsage() {
        return filters.values().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    }

    public void out() {
        RedisIO.putFilters(0, filters);
    }

    @Override
    public IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) {
        return getAndCreateIfNoExists(index);
    }
}
