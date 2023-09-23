package org.urbcomp.startdb.stkq.filter;

import org.junit.Assert;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class STFilter extends AbstractSTFilter {
    private final BasicFilterManager filterManager;

    public STFilter(int log2Size, int bitsPerKey, int sBits, int tBits) {
        super(sBits, tBits);
        filterManager = new BasicFilterManager(log2Size, bitsPerKey);
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

    @Override
    public boolean check(STObject stObject) {
        long s = sKeyGenerator.toNumber(stObject.getLocation());
        int t = tKeyGenerator.toNumber(stObject.getTime());

        BytesKey stIndex = getSTIndex(s, t);
        IFilter filter = filterManager.get(stIndex);
        for (String keyword : stObject.getKeywords()) {
            Assert.assertTrue(filter.check(ByteUtil.concat(kKeyGenerator.toBytes(keyword), getSKey(s), getTKey(t))));
        }
        return true;
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
                    if (checkInFilter(filterManager.get(getSTIndex(s, t)), stKey, kKeys, queryType)) {
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
}
