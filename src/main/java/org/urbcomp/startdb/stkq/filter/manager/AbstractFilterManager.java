package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;

public class AbstractFilterManager implements IFilterManager {
    protected final int log2Size;
    protected final int bitsPerKey;
    protected final long maxRamUsage;
    protected long ramUsage;
    protected String tableName;

    public AbstractFilterManager(int log2Size, int bitsPerKey, String tableName, long maxRamUsage) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
        this.tableName = tableName;
        this.maxRamUsage = maxRamUsage;
    }

    public long ramUsage() {
        return ramUsage;
    }

    public void out() {

    }

    @Override
    public IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) throws IOException {
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
