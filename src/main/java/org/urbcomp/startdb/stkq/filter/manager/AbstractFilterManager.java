package org.urbcomp.startdb.stkq.filter.manager;

public class AbstractFilterManager {
    protected final int log2Size;
    protected final int bitsPerKey;
    public AbstractFilterManager(int log2Size, int bitsPerKey) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
    }
}
