package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

public class InfiniFilter implements IFilter {
    private final ChainedInfiniFilter filter;

    public InfiniFilter(int log2Size, int bitsPerKey) {
        filter = new ChainedInfiniFilter(log2Size, bitsPerKey);
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

    public boolean sacrifice() { return filter.sacrifice(); }
}
