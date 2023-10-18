package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;

public class FilterWithIdx {
    final IFilter filter;
    int idx;

    public FilterWithIdx(IFilter filter, int idx) {
        this.filter = filter;
        this.idx = idx;
    }

    public IFilter getFilter() {
        return filter;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    @Override
    public String toString() {
        return "FilterWithIdx{" +
                "filter=" + filter +
                ", idx=" + idx +
                '}';
    }
}
