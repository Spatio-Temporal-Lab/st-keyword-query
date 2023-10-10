package org.urbcomp.startdb.stkq.filter.manager;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.io.HBaseWriter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BasicFilterManager extends AbstractFilterManager {
    protected final Map<BytesKey, IFilter> filters = new HashMap<>();

    public BasicFilterManager(int log2Size, int bitsPerKey) {
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

    public long size() {
        long size = 0;
        for (Map.Entry<BytesKey, IFilter> filterEntry : filters.entrySet()) {
            size += RamUsageEstimator.sizeOf(filterEntry.getValue());
        }
        return size;
    }

    public void out() throws IOException {
        String tableName = "basicFilters";
        HBaseWriter.putFilters(tableName, filters);
    }
}
