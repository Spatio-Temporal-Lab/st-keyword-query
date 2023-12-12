package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.manager.AbstractFilterManager;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StreamLRUFM extends AbstractFilterManager {
    protected final long MAX_RAM_USAGE =  50 * 1024;
    protected Map<BytesKey, IFilter> filters = new LinkedHashMap<>(100_0000, .75F, true);
    protected long ramUsage;
    protected String tableName;

    public StreamLRUFM(int log2Size, int bitsPerKey, String tableName) {
        super(log2Size, bitsPerKey);
        this.tableName = tableName;
    }

    public void doClear() throws IOException {
        ramUsage = ramUsage();
        if (ramUsage < MAX_RAM_USAGE) return;
        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();

        Map<BytesKey, IFilter> filtersToRemove = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();

            IFilter filterToRemove = entry.getValue();
            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
            filtersToRemove.put(entry.getKey(), filterToRemove);

            iterator.remove();

            if (ramUsage < MAX_RAM_USAGE) {
                break;
            }
        }

        HBaseIO.putFilters(tableName, filtersToRemove);
    }

    public void doClear(IFilter filter) throws IOException {
        ramUsage += RamUsageEstimator.sizeOf(filter);
        if (ramUsage < MAX_RAM_USAGE) return;
        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();
            byte[] key = entry.getKey().getArray();

            IFilter filterToRemove = entry.getValue();
            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
            if (HBaseIO.getFilter(tableName, key) == null) {
                HBaseIO.putFilter(tableName, key, filterToRemove);
            }

            iterator.remove();

            if (ramUsage < MAX_RAM_USAGE) {
                break;
            }
        }
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) throws IOException {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(index, filter);
        }
        return filter;
    }

    public IFilter get(BytesKey index) throws IOException {
        IFilter filter = filters.get(index);
        if (filter == null) {
            filter = HBaseIO.getFilter(tableName, index.getArray());
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
