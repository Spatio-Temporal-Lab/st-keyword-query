package org.urbcomp.startdb.stkq.stream;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilterWithTag;
import org.urbcomp.startdb.stkq.filter.manager.IFilterManager;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StreamLRUFilterManager implements IFilterManager {
    private final int log2Size;
    private final int bitsPerKey;
    private final long maxRamUsage;
    private long ramUsage;
    private final String tableName;

    private final Map<BytesKey, InfiniFilterWithTag> filters = new LinkedHashMap<>(100_0000, .75F, true);

    public StreamLRUFilterManager(int log2Size, int bitsPerKey, String tableName, long maxRamUsage) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
        this.tableName = tableName;
        this.maxRamUsage = maxRamUsage;
    }

    /**
     * 批量构建布隆过滤器后，校验是否需要清除
     * @throws IOException 存储多余的布隆过滤器抛出异常
     */
    public void doClearAfterBatchInsertion() throws IOException {
        ramUsage = ramUsage();
        clearAction();
    }

    public IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) throws IOException {
        InfiniFilterWithTag filter;
        if (!readFromDb) {
            filter = filters.get(index);
        } else {
            filter = (InfiniFilterWithTag) get(index);
        }
        if (filter == null) {
            filter = new InfiniFilterWithTag(log2Size, bitsPerKey, true);
            filters.put(index, filter);
        }
        return filter;
    }

    public IFilter get(BytesKey index) throws IOException {
        InfiniFilterWithTag filter = filters.get(index);
        if (filter == null) {
            ChainedInfiniFilter temp = HBaseIO.getFilterInChainType(tableName, index.getArray());
            if (temp != null) {
                filter = new InfiniFilterWithTag(temp, false);
                filters.put(index, filter);
                doClearAfterLoadAFilter(filter);
            }
        }
        return filter;
    }

    @Override
    public IFilter getWithIO(BytesKey index) {
        // we do nothing
        return null;
    }

    @Override
    public void out() {
        // we do nothing
    }

    @Override
    public long ramUsage() {
        return filters.values().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    }

    private void clearAction() throws IOException {
        if (ramUsage < maxRamUsage) return;

        Iterator<Map.Entry<BytesKey, InfiniFilterWithTag>> iterator = filters.entrySet().iterator();
        Map<BytesKey, IFilter> filtersToRemove = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, InfiniFilterWithTag> entry = iterator.next();

            InfiniFilterWithTag filterToRemove = entry.getValue();
            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);

            if (filterToRemove.shouldWrite()) {
                filtersToRemove.put(entry.getKey(), filterToRemove);
            }

            iterator.remove();

            if (ramUsage < maxRamUsage) {
                break;
            }
        }
        HBaseIO.putFilters(tableName, filtersToRemove);
    }

    private void doClearAfterLoadAFilter(IFilter filter) throws IOException {
        ramUsage += RamUsageEstimator.sizeOf(filter);
        clearAction();
    }
}
