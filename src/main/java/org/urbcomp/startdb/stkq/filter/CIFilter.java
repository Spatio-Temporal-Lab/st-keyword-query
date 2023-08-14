package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

public class CIFilter extends AbstractFilter {
    private final ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 12);

    public CIFilter() {
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
