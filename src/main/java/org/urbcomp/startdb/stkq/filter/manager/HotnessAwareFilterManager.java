package org.urbcomp.startdb.stkq.filter.manager;

import com.google.common.collect.TreeMultiset;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.*;

class FilterWithHotness implements Comparable<FilterWithHotness> {
    private final IFilter filter;
    private long hotness;

    public FilterWithHotness(IFilter filter) {
        this.filter = filter;
        hotness = 0;
    }

    public long getHotness() {
        return hotness;
    }

    public void setHotness(long hotness) {
        this.hotness = hotness;
    }

    public IFilter getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "FilterWithHotness{" +
                "filter=" + filter +
                ", hotness=" + hotness +
                '}';
    }

    @Override
    public int compareTo(FilterWithHotness filterWithHotness) {
        return Long.compare(hotness, filterWithHotness.hotness);
    }
}

public class HotnessAwareFilterManager extends AbstractFilterManager {
    private int queryCount = 0;
    private static final int UPDATE_TIME = 600;
    private static final int MAX_UPDATE_TIME = 3000;
//    private final Map<BytesKey, Long> st2Count = new HashMap<>();
    private final Map<BytesKey, FilterWithHotness> filters = new HashMap<>();
    private final TreeMap<Long, Set<IFilter>> sortedFilters = new TreeMap<>();

    public HotnessAwareFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(index, new FilterWithHotness(filter));
        }
//        st2Count.merge(index, 1L, Long::sum);
        return filter;
    }

    public void build() {
        for (Map.Entry<BytesKey, FilterWithHotness> entry : filters.entrySet()) {
            BytesKey index = entry.getKey();
            FilterWithHotness filter = entry.getValue();

            long count = filter.getFilter().size();
//            long count = st2Count.get(index);
            filter.setHotness(count);

            Set<IFilter> filterSet = sortedFilters.computeIfAbsent(count, k -> new HashSet<>());
            filterSet.add(filter.getFilter());
        }
//        System.out.println("st2Count: " + RamUsageEstimator.humanSizeOf(st2Count));
        System.out.println("filters: " + RamUsageEstimator.humanSizeOf(filters));
        System.out.println("sorted filters: " + RamUsageEstimator.humanSizeOf(sortedFilters));
        System.out.println("size = " + filters.size());
        System.out.println(RamUsageEstimator.humanSizeOf(this));
    }

    public IFilter update(BytesKey index) {
        FilterWithHotness filter = filters.get(index);

        if (filter == null) {
            return null;
        }

        ++queryCount;

        long oldHotness = filter.getHotness();
        Set<IFilter> oldFilterSet = sortedFilters.get(oldHotness);
        oldFilterSet.remove(filter.getFilter());
        if (oldFilterSet.isEmpty()) {
            sortedFilters.remove(oldHotness);
        }

//        long newHotness = oldHotness + st2Count.get(index);
        long newHotness = filter.getFilter().size();
        filter.setHotness(newHotness);

        Set<IFilter> newFilterSet = sortedFilters.computeIfAbsent(newHotness, k -> new HashSet<>());
        newFilterSet.add(filter.getFilter());

        if (queryCount % UPDATE_TIME == 0 && queryCount <= MAX_UPDATE_TIME) {

            System.out.println("filters size 0: " + RamUsageEstimator.humanSizeOf(filters));
            System.out.println("sorted filters size 0: " + RamUsageEstimator.humanSizeOf(sortedFilters));

            int size = filters.size();
            int upSize = size * 3 / 4;

            int i = 0;
            System.out.println("-------------------------");
            for (Map.Entry<Long, Set<IFilter>> filterEntry : sortedFilters.entrySet()) {
                boolean end = false;

//                System.out.println(filterEntry.getKey());
                Set<IFilter> set = filterEntry.getValue();
                for (IFilter filter_ : set) {
                    if (++i <= upSize) {
                        long size1 = RamUsageEstimator.sizeOf(filter_);
//                        System.out.println("before: " + size1);
//                        set.remove(filter_);
                        filter_.sacrifice();
//                        set.add(filter_);
                        long size2 = RamUsageEstimator.sizeOf(filter_);
//                        System.out.println("after: " + size2);
//                        System.out.println(size1 + " " + size2);
                        if (size2 > size1) {
                            System.err.println("error");
                        }
                    } else {
                        end = true;
                        break;
                    }
                }
                if (end) {
                    break;
                }
            }


            System.out.println("filters size 1: " + RamUsageEstimator.humanSizeOf(filters));
            System.out.println("sorted filters size 1: " + RamUsageEstimator.humanSizeOf(sortedFilters));
        }

        return filter.getFilter();
    }

    public IFilter get(BytesKey index) {
        FilterWithHotness filterWithHotness = filters.get(index);
        return filterWithHotness == null ? null : filterWithHotness.getFilter();
    }

    public IFilter getAndUpdate(BytesKey index) {
        return update(index);
    }

    public static void main(String[] args) {
        TreeMultiset<FilterWithHotness> set = TreeMultiset.create();
        Random random = new Random();

        int n = 3;
        FilterWithHotness[] filters = new FilterWithHotness[n];
        for (int i = 0; i < n; ++i) {
            filters[i] = new FilterWithHotness(new InfiniFilter());
            filters[i].setHotness(i);
            set.add(filters[i]);
        }
        int i = 0;
        for (FilterWithHotness filter : set) {
            if (++i == 11) {
                break;
            }
            System.out.println("1: " + set);
            set.remove(filter);
            System.out.println("2: " + set);
            filter.setHotness(filter.getHotness() + n - 1);
            set.add(filter);
            System.out.println("3: " + set);
        }
        System.out.println(set);
    }

    public Map<BytesKey, FilterWithHotness> getFilters() {
        return filters;
    }

    public long size() {
        long size = 0;
        for (Map.Entry<BytesKey, FilterWithHotness> filter : filters.entrySet()) {
            size += RamUsageEstimator.sizeOf(filter.getValue().getFilter());
        }
        return size;
    }
}
