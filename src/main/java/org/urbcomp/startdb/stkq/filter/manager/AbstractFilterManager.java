package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;

public class AbstractFilterManager implements IFilterManager {
    protected final int log2Size;
    protected final int bitsPerKey;

    public AbstractFilterManager(int log2Size, int bitsPerKey) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
    }

    public long ramUsage() {
        return 0;
    }

    public void out() {

    }

    @Override
    public IFilter getAndCreateIfNoExists(BytesKey index) throws IOException {
        return null;
    }

    @Override
    public IFilter get(BytesKey index) throws IOException {
        return null;
    }

    @Override
    public IFilter getWithIO(BytesKey index) {
        return null;
    }
}
