package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.manager.AbstractFilterManager;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StreamLRUFilterManager extends AbstractFilterManager {
    private long ramUsage = 0;
    private final long MAX_RAM_USAGE = 50 * 1024 * 1024;
    protected Map<BytesKey, IFilter> filters = new LinkedHashMap<>(100_0000, .75F, true);

    public StreamLRUFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public void doClear() {
        if (ramUsage < MAX_RAM_USAGE) return;
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
    private void doClear(IFilter filter) {
        ramUsage += RamUsageEstimator.sizeOf(filter);
        if (ramUsage < MAX_RAM_USAGE) return;
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

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(index, filter);
            doClear(filter);
        }
        return filter;
    }

    public IFilter get(BytesKey index) {
        IFilter filter = filters.get(index);
        if (filter == null) {
            filter = RedisIO.getFilter(0, index.getArray());
            if (filter != null) {
                filters.put(index, filter);
                doClear(filter);
            }
            return filter;
        }
        return filter;
    }

    @Override
    public long ramUsage() {
        long size = 0;
        for (Map.Entry<BytesKey, IFilter> filterEntry : filters.entrySet()) {
            size += RamUsageEstimator.sizeOf(filterEntry.getValue());
        }
        return size;
    }
}
