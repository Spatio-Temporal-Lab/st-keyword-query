package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.model.Range;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class BasicRosetta {
    protected int n;
    protected List<ChainedInfiniFilter> filters;

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
        n = 3;
    }

    public void writeTo(String fileNamePre) throws IOException {
        for (int i = 0; i < n; ++i) {
            OutputStream output = Files.newOutputStream(Paths.get(fileNamePre + "_" + i));
            for (ChainedInfiniFilter filter : filters) {
                filter.writeTo(output);
            }
        }
    }

    public BasicRosetta read(String fileNamePre, int n) throws IOException {
        this.n = n;
        this.filters = new ArrayList<>(n);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        for (int i = 0; i < n; ++i) {
            InputStream in = Files.newInputStream(Paths.get(fileNamePre + "_" + i));
            filters.add(temp.read(in));
        }
        return this;
    }

    public ArrayList<Long> filter(Range<Integer> tRange, Range<Long> sRange) { return new ArrayList<>(); }

    public ArrayList<Range<byte[]>> filter(ArrayList<Range<Long>> sRanges, Range<Integer> tRange, ArrayList<byte[]> keyPre) {return new ArrayList<>();}

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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(n);
        for (int i = 0; i < n; ++i) {
            builder.append(" ").append(RamUsageEstimator.humanSizeOf(filters.get(i)));
        }
        return builder.toString();
    }
}
