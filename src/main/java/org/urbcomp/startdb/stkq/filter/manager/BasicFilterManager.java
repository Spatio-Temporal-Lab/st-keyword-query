package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.SetFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.HashMap;
import java.util.Map;

public class BasicFilterManager extends AbstractFilterManager {
    protected final Map<BytesKey, IFilter> filters = new HashMap<>();

    public BasicFilterManager(int log2Size, int bitsPerKey) {
        super(log2Size, bitsPerKey);
    }

    public IFilter getAndCreateIfNoExists(BytesKey index) {
        IFilter filter = get(index);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
//            filter = new SetFilter();
            filters.put(index, filter);
        }
        return filter;
    }

    public IFilter get(BytesKey index) {
        return filters.get(index);
    }
}
