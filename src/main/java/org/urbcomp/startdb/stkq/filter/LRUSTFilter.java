package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.filter.manager.LRUFilterManager;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.List;

public class LRUSTFilter extends AbstractSTFilter {
    private final LRUFilterManager filterManager;
    private boolean first = true;

    public LRUSTFilter(int log2Size, int bitsPerKey, int sIndexBits, int tIndexBits) {
        super(sIndexBits, tIndexBits);
        filterManager = new LRUFilterManager(log2Size, bitsPerKey);
    }

    public void insert(STObject stObject) {
        long s = sKeyGenerator.toNumber(stObject.getLocation());
        int t = tKeyGenerator.toNumber(stObject.getTime());

        BytesKey stIndex = getSTIndex(s, t);
        IFilter filter = filterManager.getAndCreateIfNoExists(stIndex);
        for (String keyword : stObject.getKeywords()) {
            filter.insert(ByteUtil.concat(kKeyGenerator.toBytes(keyword), getSKey(s), getTKey(t)));
        }
    }

    public IFilter get(byte[] stIndex) {
        return filterManager.getWithIO(new BytesKey(stIndex));
    }

    public List<Range<byte[]>> shrinkWithIOAndTransform(Query query) {
        return super.shrinkWithIOAndTransform(query, 1);
    }

    @Override
    public IFilter getWithIO(byte[] stIndex) {
        return filterManager.getWithIO(new BytesKey(stIndex));
    }

    @Override
    public void out() throws IOException {
        filterManager.out();
    }

    @Override
    public long size() {
        return filterManager.size();
    }
}
