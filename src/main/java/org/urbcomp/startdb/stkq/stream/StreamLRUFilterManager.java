package org.urbcomp.startdb.stkq.stream;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilterWithTag;
import org.urbcomp.startdb.stkq.filter.manager.AbstractFilterManager;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StreamLRUFilterManager extends AbstractFilterManager {

    private final Map<BytesKey, InfiniFilterWithTag> filters = new LinkedHashMap<>(100_0000, .75F, true);

    public StreamLRUFilterManager(int log2Size, int bitsPerKey, String tableName, long maxRamUsage) {
        super(log2Size, bitsPerKey, tableName, maxRamUsage);
    }

    /**
     * 批量构建布隆过滤器后，校验是否需要清除
     * @throws IOException 存储多余的布隆过滤器抛出异常
     */
    public void doClearAfterBatchInsertion() throws IOException {
        ramUsage = ramUsage();
        clearAction();
    }

    /**
     * 从外存中加载布隆过滤器后，校验是否超过了内存限制，如果超过了，清除内存中多余的布隆过滤器
     * @param filter 新添加的布隆过滤器
     * @throws IOException 存储多余的布隆过滤器抛出异常
     */
    private void doClearAfterLoadAFilter(IFilter filter) throws IOException {
        ramUsage += RamUsageEstimator.sizeOf(filter);
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
}
