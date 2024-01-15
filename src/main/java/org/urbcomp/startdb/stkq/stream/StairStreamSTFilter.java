package org.urbcomp.startdb.stkq.stream;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.AbstractSTKFilter;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilterWithTag;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StairStreamSTFilter extends AbstractSTKFilter {
    private final StairStreamLRUFilterManager filterManager;

    protected Set<BytesKey> fnSet = new HashSet<>();

    private int latestTimeBin = 0;

    public StairStreamSTFilter(int sBits, int tBits, StairStreamLRUFilterManager filterManager) {
        super(sBits, tBits);
        this.filterManager = filterManager;
    }

    public void insert(STObject stObject) throws IOException {
        long s = sKeyGenerator.toNumber(stObject.getLocation());
        int t = tKeyGenerator.toNumber(stObject.getTime());
        BytesKey stIndex = getSTIndex(s, t);
        int tIndex = t >> tBits;

        IFilter filter;
        if (tIndex >= latestTimeBin) {
            //Here we assume that the filter in the latest timeBin must be in memory, so we will not get the filter from HBase
            if (tIndex > latestTimeBin) {
                filterManager.clearForNewWindow();
            }
            latestTimeBin = tIndex;
            filter = filterManager.getAndCreateIfNoExists(stIndex, 0, false);
        } else  {
            // Out of bounds errors will be checked in filterManager
            filter = filterManager.getAndCreateIfNoExists(stIndex, latestTimeBin - tIndex, true);
        }

        for (String keyword : stObject.getKeywords()) {
            byte[] key = ByteUtil.concat(kKeyGenerator.toBytes(keyword), getSKey(s), getTKey(t));
            if (!filter.insert(key)) {
                fnSet.add(new BytesKey(key));
            } else if (filter instanceof InfiniFilterWithTag) {
                ((InfiniFilterWithTag) filter).setWriteTag(true);
            }
        }
    }

    public List<Range<byte[]>> shrinkAndMerge(Query query) throws IOException {
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();

        int tIndexMin = tLow >> tBits;
        int tIndexMax = tHigh >> tBits;

        QueryType queryType = query.getQueryType();
        List<byte[]> kKeys = query.getKeywords().stream().map(kKeyGenerator::toBytes).collect(Collectors.toList());

        ArrayList<Long> keysLong = new ArrayList<>();

        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();

            long sIndexMin = sLow >> sBits;
            long sIndexMax = sHigh >> sBits;

            for (long sIndex = sIndexMin; sIndex <= sIndexMax; ++sIndex) {
                for (int tIndex = tIndexMin; tIndex <= tIndexMax; ++tIndex) {

                    if (tIndex > latestTimeBin) {
                        break;
                    }

                    //calculate which grid the key is in
                    byte[] stIndex = ByteUtil.concat(ByteUtil.getKByte(sIndex, sIndexBytes), ByteUtil.getKByte(tIndex, tIndexBytes));
                    IFilter filter = filterManager.get(new BytesKey(stIndex), latestTimeBin - tIndex);
                    if (filter == null) {
                        continue;
                    }

                    long sMin = Math.max(sIndex << sBits, sLow);
                    long sMax = Math.min(sIndex << sBits | sMask, sHigh);
                    int tMin = Math.max(tIndex << tBits, tLow);
                    int tMax = Math.min(tIndex << tBits | tMask, tHigh);

                    // traverse the keys in this grid that intersect the query range
                    for (long s = sMin; s <= sMax; ++s) {
                        for (int t = tMin; t <= tMax; ++t) {
                            byte[] stKey = ByteUtil.concat(getSKey(s), getTKey(t));
                            if (checkInFilter(filter, fnSet, stKey, kKeys, queryType)) {
                                keysLong.add(s << tKeyGenerator.getBits() | t);
                            }
                        }
                    }
                }
            }
        }

        return merge(keysLong);
    }

    @Override
    public long ramUsage() {
        return RamUsageEstimator.sizeOf(this);
    }

    @Override
    public void out() {
    }

    @Override
    public List<Range<Long>> shrinkAndMergeLong(Query query) {
        return null;
    }

    @Override
    public IFilter getWithIO(byte[] stIndex) {
        return null;
    }

    public void doClear() {
        filterManager.reCalculateRamUsage();
    }
}
