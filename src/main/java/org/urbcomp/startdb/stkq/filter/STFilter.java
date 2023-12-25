package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.manager.IFilterManager;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class STFilter extends AbstractSTFilter implements ISTKFilter {
    private final IFilterManager filterManager;

    public STFilter(int sBits, int tBits, IFilterManager filterManager) {
        super(sBits, tBits);
        this.filterManager = filterManager;
    }

    @Override
    public void insert(STObject stObject) throws IOException {
        long s = sKeyGenerator.toNumber(stObject.getLocation());
        int t = tKeyGenerator.toNumber(stObject.getTime());

        BytesKey stIndex = getSTIndex(s, t);
        IFilter filter = filterManager.getAndCreateIfNoExists(stIndex, true);
        for (String keyword : stObject.getKeywords()) {
            filter.insert(ByteUtil.concat(kKeyGenerator.toBytes(keyword), getSKey(s), getTKey(t)));
        }
    }

    public List<byte[]> shrink(Query query) throws IOException {
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

    public List<byte[]> shrinkWithIO(Query query) {
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();

        List<byte[]> results = new ArrayList<>();
        QueryType queryType = query.getQueryType();
        List<byte[]> kKeys = query.getKeywords().stream().map(kKeyGenerator::toBytes).collect(Collectors.toList());

        int tIndexMin = tLow >> tBits;
        int tIndexMax = tHigh >> tBits;

        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();

            long sIndexMin = sLow >> sBits;
            long sIndexMax = sHigh >> sBits;

            for (long sIndex = sIndexMin; sIndex <= sIndexMax; ++sIndex) {
                for (int tIndex = tIndexMin; tIndex <= tIndexMax; ++tIndex) {
                    byte[] stIndex = ByteUtil.concat(ByteUtil.getKByte(sIndex, sIndexBytes), ByteUtil.getKByte(tIndex, tIndexBytes));
                    IFilter filter = RedisIO.getFilter(stIndex);

                    if (filter == null) {
                        continue;
                    }

                    long sMin = Math.max(sIndex << sBits, sLow);
                    long sMax = Math.min(sIndex << sBits | sMask, sHigh);

                    int tMin = Math.max(tIndex << tBits, tLow);
                    int tMax = Math.min(tIndex << tBits | tMask, tHigh);

                    for (long s = sMin; s <= sMax; ++s) {
                        for (int t = tMin; t <= tMax; ++t) {
                            byte[] stKey = ByteUtil.concat(getSKey(s), getTKey(t));
                            if (checkInFilter(filter, stKey, kKeys, queryType)) {
                                results.add(ByteUtil.concat(
                                        sKeyGenerator.numberToBytes(s),
                                        tKeyGenerator.numberToBytes(t)
                                ));
                            }
                        }
                    }

                }
            }
        }

        return results;
    }

    @Override
    public IFilter getWithIO(byte[] stIndex) {
        return filterManager.getWithIO(new BytesKey(stIndex));
    }

    @Override
    public long ramUsage() {
        return filterManager.ramUsage();
    }

    @Override
    public void out() {
        filterManager.out();
    }
}
