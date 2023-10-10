package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

import java.io.ByteArrayOutputStream;

public class InfiniFilter implements IFilter {
    private final ChainedInfiniFilter filter;

    public InfiniFilter() {
        filter = new ChainedInfiniFilter(3, 12);
        filter.set_expand_autonomously(true);
    }

    public InfiniFilter(int log2Size, int bitsPerKey) {
        filter = new ChainedInfiniFilter(log2Size, bitsPerKey);
        filter.set_expand_autonomously(true);
    }

    public InfiniFilter(ChainedInfiniFilter filter) {
        this.filter = filter;
    }

    @Override
    public boolean check(byte[] key) {
        return filter.search(key);
    }

    @Override
    public int size() {
        return (int) filter.get_num_existing_entries();
    }

    @Override
    public void insert(byte[] code) {
        filter.insert(code, true);
    }

    public boolean sacrifice() { return filter.sacrifice(); }

    @Override
    public int appSize() {
        return filter.getNum_expansions();
    }

    @Override
    public void writeTo(ByteArrayOutputStream bos) {
        filter.writeTo(bos);
    }
}
