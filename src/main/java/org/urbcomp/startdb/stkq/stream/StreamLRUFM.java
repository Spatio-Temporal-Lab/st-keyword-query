package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.manager.AbstractFilterManager;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StreamLRUFM extends AbstractFilterManager {
    protected final long MAX_RAM_USAGE =  5 * 1024 * 1024;
    protected Map<BytesKey, IFilter> filters = new LinkedHashMap<>(100_0000, .75F, true);

    public StreamLRUFM(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public void doClear() throws IOException {
        long ramUsage = RamUsageEstimator.sizeOf(filters);
        if (ramUsage < MAX_RAM_USAGE) return;
        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();
            byte[] key = entry.getKey().getArray();

            IFilter filterToRemove = entry.getValue();
            iterator.remove();
            ramUsage = RamUsageEstimator.sizeOf(filters);

//            if (RedisIO.get(0, key) == null) {
//                RedisIO.putFilter(0, key, filterToRemove);
//            }
            if (HBaseIO.getFilter("filters", key) == null) {
                HBaseIO.putFilter("filters", key, filterToRemove);
            }

//            System.out.println("????");

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
//            filter = RedisIO.getFilter(0, index.getArray());
            filter = HBaseIO.getFilter("filters", index.getArray());
            if (filter != null) {
                filters.put(index, filter);
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
