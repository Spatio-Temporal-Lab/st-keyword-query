package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.manager.LRUFilterManager;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LRUSTFilter extends AbstractSTFilter {
    private final LRUFilterManager filterManager;

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

    public List<byte[]> shrink(Query query) {

        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();

        List<byte[]> results = new ArrayList<>();
        QueryType queryType = query.getQueryType();
        List<byte[]> kKeys = query.getKeywords().stream().map(kKeyGenerator::toBytes).collect(Collectors.toList());

        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();
            for (long s = sLow; s <= sHigh; ++s) {
                for (int t = tLow; t <= tHigh; ++t) {
                    byte[] stKey = ByteUtil.concat(getSKey(s), getTKey(t));
                    if (checkInFilter(filterManager.getAndUpdate(getSTIndex(s, t)), stKey, kKeys, queryType)) {
                        results.add(ByteUtil.concat(
                                sKeyGenerator.numberToBytes(s),
                                tKeyGenerator.numberToBytes(t)
                        ));
                    }
                }
            }
        }

        return results;
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

    public void compress() {
        filterManager.compress();
    }
}
