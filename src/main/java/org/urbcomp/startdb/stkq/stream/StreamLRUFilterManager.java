package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.manager.AbstractFilterManager;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.*;

public class StreamLRUFilterManager extends AbstractFilterManager {
    protected final Map<BytesKey, IFilter> filters = new LinkedHashMap<>(100_0000, .75F, true);

    public StreamLRUFilterManager(int log2Size, int bitsPerKey, String tableName, long maxRamUsage) {
        super(log2Size, bitsPerKey, tableName, maxRamUsage);
    }

    /**
     * 批量构建布隆过滤器后，校验是否需要清除
     * @throws IOException 存储多余的布隆过滤器抛出异常
     */
    public void doClear() throws IOException {
        ramUsage = ramUsage();
        if (ramUsage < maxRamUsage) return;

        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();
        Map<BytesKey, IFilter> filtersToRemove = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();

            IFilter filterToRemove = entry.getValue();
            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
            filtersToRemove.put(entry.getKey(), filterToRemove);

            iterator.remove();

            if (ramUsage < maxRamUsage) {
                break;
            }
        }

        HBaseIO.putFilters(tableName, filtersToRemove);
    }

    /**
     * 从外存中加载布隆过滤器后，校验是否超过了内存限制，如果超过了，清除内存中多余的布隆过滤器
     * @param filter 新添加的布隆过滤器
     * @throws IOException 存储多余的布隆过滤器抛出异常
     */
    protected void doClear(IFilter filter) throws IOException {
        ramUsage += RamUsageEstimator.sizeOf(filter);
        if (ramUsage < maxRamUsage) return;

        Iterator<Map.Entry<BytesKey, IFilter>> iterator = filters.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, IFilter> entry = iterator.next();
            byte[] key = entry.getKey().getArray();

            IFilter filterToRemove = entry.getValue();
            ramUsage -= RamUsageEstimator.sizeOf(filterToRemove);
            HBaseIO.putFilterIfNotExist(tableName, key, filterToRemove);

            iterator.remove();

            if (ramUsage < maxRamUsage) {
                break;
            }
        }
    }

    public IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) {
        IFilter filter = filters.get(index);
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
        }
        return filter;
    }

    @Override
    public long ramUsage() {
        return filters.values().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    }
}
