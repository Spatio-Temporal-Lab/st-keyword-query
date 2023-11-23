package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

public class AbstractFilterManager implements IFilterManager {
    protected final int log2Size;
    protected final int bitsPerKey;
    public AbstractFilterManager(int log2Size, int bitsPerKey) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
    }

    public int getUpdateSize(int size, int k) {
        return size - k * 50000;
    }

    public long size() {
        return 0;
    }

    public void compress() {

    }

    public void out() {

    }

    @Override
    public IFilter getAndCreateIfNoExists(BytesKey index) {
        return null;
    }

    @Override
    public IFilter get(BytesKey index) {
        return null;
    }

    @Override
    public IFilter getWithIO(BytesKey index) {
        return null;
    }

    @Override
    public IFilter update(BytesKey index) {
        return null;
    }

    @Override
    public IFilter getAndUpdate(BytesKey index) {
        return update(index);
    }
}
