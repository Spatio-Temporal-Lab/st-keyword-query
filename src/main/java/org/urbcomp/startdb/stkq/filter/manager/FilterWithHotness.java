package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;

import java.io.ByteArrayOutputStream;

public class FilterWithHotness implements Comparable<FilterWithHotness> {
    private final IFilter filter;
    private long hotness;

    public FilterWithHotness(IFilter filter) {
        this.filter = filter;
        hotness = filter.size();
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

    public void writeTo(ByteArrayOutputStream bos) {
        filter.writeTo(bos);
    }
}
