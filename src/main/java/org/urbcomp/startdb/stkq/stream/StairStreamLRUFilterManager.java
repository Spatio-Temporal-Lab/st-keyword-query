package org.urbcomp.startdb.stkq.stream;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilterWithTag;
import org.urbcomp.startdb.stkq.filter.manager.IFilterManager;
import org.urbcomp.startdb.stkq.io.LevelDbIO;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StairStreamLRUFilterManager implements IFilterManager {
    private final int level;
    //0 is the newest lru queue
    private final Map<BytesKey, InfiniFilterWithTag>[] lruQueues;
    private final long[] maxRamUsages;
    private final long[] ramUsages;
    private final int log2Size;
    private final int bitsPerKey;

    public StairStreamLRUFilterManager(int log2Size, int bitsPerKey, long[] ramUsageDistribution) {
        level = ramUsageDistribution.length;
        maxRamUsages = ramUsageDistribution;
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
        lruQueues = new Map[level];
        ramUsages = new long[level];
        for (int i = 0; i < level; ++i) {
            lruQueues[i] = new LinkedHashMap<>(16, .75F, true);
        }
    }

    private void clearOneLevel(int t) throws IOException {
        ramUsages[t] = ramUsage(t);
        clearAction(t);
    }

    private void clearWithoutCheck(int t) throws IOException {
        Map<BytesKey, InfiniFilterWithTag> lruQueue = lruQueues[t];
        Iterator<Map.Entry<BytesKey, InfiniFilterWithTag>> iterator = lruQueue.entrySet().iterator();
        Map<BytesKey, IFilter> filtersToRemove = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, InfiniFilterWithTag> entry = iterator.next();
            byte[] key = entry.getKey().getArray();
            InfiniFilterWithTag filterToRemove = entry.getValue();
            if (filterToRemove.shouldWrite()) {
                filtersToRemove.put(new BytesKey(key), filterToRemove);
            }
            iterator.remove();
        }
        LevelDbIO.putFilters(filtersToRemove);
    }

    /**
     * 新的时间窗口产生，将部分老的过滤器flush掉
     * @throws IOException 存储多余的布隆过滤器抛出异常
     */
    public void clearForNewWindow() throws IOException {
        // Put the oldest filters into db
        clearWithoutCheck(level - 1);
        // Each queue become smaller
        for (int i = level - 1; i > 0; --i) {
            lruQueues[i] = lruQueues[i - 1];
        }
        for (int i = 1; i < level; ++i) {
            clearOneLevel(i);
        }
        lruQueues[0] = new LinkedHashMap<>(16, .75F, true);
    }

    /**
     * 从外存中加载布隆过滤器后，校验是否超过了内存限制，如果超过了，清除内存中多余的布隆过滤器
     * @param filter 新添加的布隆过滤器
     */
    protected void doClearAfterLoadAFilter(IFilter filter, int t) throws IOException {
        ramUsages[t] += RamUsageEstimator.sizeOf(filter);
        clearAction(t);
    }

    public IFilter getAndCreateIfNoExists(BytesKey index, int t, boolean readFromDb) throws IOException {
        Map<BytesKey, InfiniFilterWithTag> lruQueue = lruQueues[Math.min(t, level - 1)];
        InfiniFilterWithTag filter;
        if (!readFromDb) {
            filter = lruQueue.get(index);
        } else {
            filter = (InfiniFilterWithTag) get(index);
        }
        if (filter == null) {
            filter = new InfiniFilterWithTag(log2Size, bitsPerKey, true);
            lruQueue.put(index, filter);
        }
        return filter;
    }

    public IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) throws IOException {
        return getAndCreateIfNoExists(index, 0, readFromDb);
    }

    @Override
    public IFilter get(BytesKey index) throws IOException {
        return null;
    }

    @Override
    public IFilter getWithIO(BytesKey index) {
        return null;
    }

    @Override
    public void out() {

    }

    public IFilter get(BytesKey index, int t) throws IOException {
        t = Math.min(t, level - 1);
        Map<BytesKey, InfiniFilterWithTag> lruQueue = lruQueues[t];
        InfiniFilterWithTag filter = lruQueue.get(index);
        if (filter == null) {
            ChainedInfiniFilter temp = LevelDbIO.getFilterInChainType(index.getArray());
            if (temp != null) {
                filter = new InfiniFilterWithTag(temp, false);
                lruQueue.put(index, filter);
                doClearAfterLoadAFilter(filter, t);
            }
        }
        return filter;
    }

    @Override
    public long ramUsage() {
        long sum = 0;
        for (Map<BytesKey, InfiniFilterWithTag> lruQueue : lruQueues) {
            sum += lruQueue.values().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
        }
        return sum;
    }

    public long ramUsage(int t) {
        return lruQueues[t].values().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    }

    public void reCalculateRamUsage() {
        for (int i = 0; i < level; ++i) {
            ramUsages[i] = ramUsage(i);
        }
    }

    private void clearAction(int t) throws IOException {
        if (ramUsages[t] < maxRamUsages[t]) return;

        Map<BytesKey, InfiniFilterWithTag> filters = lruQueues[t];
        Iterator<Map.Entry<BytesKey, InfiniFilterWithTag>> iterator = filters.entrySet().iterator();
        Map<BytesKey, IFilter> filtersToRemove = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<BytesKey, InfiniFilterWithTag> entry = iterator.next();

            InfiniFilterWithTag filterToRemove = entry.getValue();
            ramUsages[t] -= RamUsageEstimator.sizeOf(filterToRemove);

            if (filterToRemove.shouldWrite()) {
                filtersToRemove.put(entry.getKey(), filterToRemove);
            }

            iterator.remove();

            if (ramUsages[t] < maxRamUsages[t]) {
                break;
            }
        }
        LevelDbIO.putFilters(filtersToRemove);
    }
}
