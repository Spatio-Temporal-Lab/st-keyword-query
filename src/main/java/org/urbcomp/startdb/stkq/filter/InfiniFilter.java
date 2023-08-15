package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

public class InfiniFilter implements IFilter {
    private final ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 15);

    public InfiniFilter() {
        filter.set_expand_autonomously(true);
    }

    @Override
    public boolean check(byte[] key) {
        return filter.search(key);
    }

    @Override
    public void insert(byte[] code) {
        filter.insert(code, false);
    }
}
