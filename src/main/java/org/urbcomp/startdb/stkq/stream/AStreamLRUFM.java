package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AStreamLRUFM extends StreamLRUFM {

    public AStreamLRUFM(int log2Size, int bitsPerKey, String tableName, long maxRamUsage) {
        super(log2Size, bitsPerKey, tableName, maxRamUsage);
    }

    public void doClear() throws IOException {
        ramUsage = ramUsage();
        System.out.println("ramUsage = " + ramUsage);
        if (ramUsage < MAX_RAM_USAGE) return;
        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();

        Map<BytesKey, IFilter> filtersToRemove = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();

            IFilter filterToRemove = entry.getValue();
            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
            filterToRemove.sacrifice();
            filtersToRemove.put(entry.getKey(), filterToRemove);

            iterator.remove();

            if (ramUsage < MAX_RAM_USAGE) {
                break;
            }
        }

        HBaseIO.putFilters("filters", filtersToRemove);
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
            filterToRemove.sacrifice();
            HBaseIO.putFilter("filters", key, filterToRemove);
            iterator.remove();

            if (ramUsage < MAX_RAM_USAGE) {
                break;
            }
        }
    }
}
