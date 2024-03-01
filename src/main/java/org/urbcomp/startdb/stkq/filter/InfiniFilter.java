package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

import java.io.ByteArrayOutputStream;

public class InfiniFilter implements IFilter {
    private final ChainedInfiniFilter filter;

    public ChainedInfiniFilter getFilter() {
        return filter;
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
    public boolean insert(byte[] code) {
        return filter.insert(code, true);
    }

    @Override
    public void writeTo(ByteArrayOutputStream bos) {
        filter.writeTo(bos);
    }

    @Override
    public String toString() {
        return String.valueOf(filter.get_fingerprint_length());
    }
}
