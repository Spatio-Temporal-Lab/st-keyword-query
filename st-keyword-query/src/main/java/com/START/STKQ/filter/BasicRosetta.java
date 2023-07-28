package com.START.STKQ.filter;


import com.START.STKQ.model.Range;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

import java.util.ArrayList;

public class BasicRosetta {
    protected int n;
    protected ArrayList<ChainedInfiniFilter> filters;

    public BasicRosetta(int n) {
        this.n = n;
        filters = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 10);
            filter.set_expand_autonomously(true);
            filters.add(filter);
        }
    }

    public BasicRosetta() {
    }

    public ArrayList<Long> filter(Range<Integer> tRange, Range<Long> sRange) { return new ArrayList<>(); }

//    public ArrayList<Long> filter(Range<Long> range) {
//
//        long originLow = range.getLow();
//        long originHigh = range.getHigh();
//        long low = originLow >> (n - 1);
//        long high = originHigh >> (n - 1);
//
//        Stream<Long> result = LongStream.range(low, high + 1).parallel().filter(filters.get(0)::search).boxed();
//
//        if (n == 1) {
//            return result.collect(Collectors.toCollection(ArrayList::new));
//        }
//
//        for (int i = 1; i < n; ++i) {
//            int shift = n - i - 1;
//
//            ChainedInfiniFilter filter = filters.get(i);
//
//            long finalLow = originLow >> shift;
//            long finalHigh = originHigh >> shift;
//
//            result = result.parallel().map(key -> {
//                ArrayList<Long> keys = new ArrayList<>();
//                long left = key << 1;
//                if (left >= finalLow) {
//                    if (filter.search(left)) {
//                        keys.add(left);
//                    }
//                }
//                long right = left | 1;
//                if (right <= finalHigh) {
//                    if (filter.search(right)) {
//                        keys.add(right);
//                    }
//                }
//                return keys;
//            }).flatMap(ArrayList::stream);
//        }
//        return result.collect(Collectors.toCollection(ArrayList::new));
//    }

    public ArrayList<Long> filter(Range<Long> range) {

        long originLow = range.getLow();
        long originHigh = range.getHigh();
        long low = originLow >> (n - 1);
        long high = originHigh >> (n - 1);

        ChainedInfiniFilter filter = filters.get(0);
        ArrayList<Long> result = new ArrayList<>();
        for (long i = low; i <= high; ++i) {
            if (filter.search(i)) {
                result.add(i);
            }
        }

        if (n == 1) {
            return result;
        }

        for (int i = 1; i < n; ++i) {
            int shift = n - i - 1;

            ArrayList<Long> temp = new ArrayList<>();
            filter = filters.get(i);

            low = originLow >> shift;
            high = originHigh >> shift;

            for (long j : result) {
                long left = j << 1;
                if (left >= low) {
                    if (filter.search(left)) {
                        temp.add(left);
                    }
                }
                long right = left | 1;
                if (right <= high) {
                    if (filter.search(right)) {
                        temp.add(right);
                    }
                }
            }

            if (temp.size() == 0) {
                return temp;
            }
            result = new ArrayList<>(temp);
        }
        return result;
    }

    public void insert(long x) {
        for (int i = 0; i < n; ++i) {
            filters.get(i).insert(x >> (n - i - 1), false);
        }
    }
}
