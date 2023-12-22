package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

import java.io.ByteArrayOutputStream;

public class InfiniFilterWithTag implements IFilter {
    private final ChainedInfiniFilter filter;
    private boolean writeTag;

    public ChainedInfiniFilter getFilter() {
        return filter;
    }

    public InfiniFilterWithTag(int log2Size, int bitsPerKey, boolean writeTag) {
        filter = new ChainedInfiniFilter(log2Size, bitsPerKey);
        filter.set_expand_autonomously(true);
        this.writeTag = writeTag;
    }

    public InfiniFilterWithTag(ChainedInfiniFilter filter, boolean writeTag) {
        this.filter = filter;
        this.writeTag = writeTag;
    }

    public boolean shouldWrite() {
        return writeTag;
    }

    public void setWriteTag(boolean tag) {
        writeTag = tag;
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

    public boolean sacrifice() { return filter.sacrifice(); }

    @Override
    public void writeTo(ByteArrayOutputStream bos) {
        filter.writeTo(bos);
    }

    @Override
    public String toString() {
        return String.valueOf(filter.get_fingerprint_length());
    }
}
