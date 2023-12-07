package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class AStreamLRUFM extends StreamLRUFM {

    public AStreamLRUFM(int log2Size, int bitsPerKey) {
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
//            System.out.println("before: " + ramUsage + " " + RamUsageEstimator.humanSizeOf(filters));
            ramUsage = RamUsageEstimator.sizeOf(filters);
//            System.out.println("after: " + ramUsage);

            filterToRemove.sacrifice();
//            RedisIO.putFilter(0, key, filterToRemove);
            HBaseIO.putFilter("filters", key, filterToRemove);
//            System.out.println("oh no");

            if (ramUsage < MAX_RAM_USAGE) {
                break;
            }
        }
    }
}
