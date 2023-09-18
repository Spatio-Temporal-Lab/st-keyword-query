package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class STFilter {
    private final int log2Size;
    private final int bitsPerKey;
    private final int sBytes;
    private final int tBytes;
    private final int sIndexBits;
    private final int tIndexBits;
    private final int sIndexBytes;
    private final int tIndexBytes;
    private final long sIndexMask;
    private final int tIndexMask;
    private final ISpatialKeyGeneratorNew sKeyGenerator = new HilbertSpatialKeyGeneratorNew();
    private final TimeKeyGeneratorNew tKeyGenerator = new TimeKeyGeneratorNew();
    private final KeywordKeyGeneratorNew kKeyGenerator = new KeywordKeyGeneratorNew();
    private final Map<BytesKey, IFilter> filters = new HashMap<>();

    public STFilter(int log2Size, int bitsPerKey, int sIndexBits, int tIndexBits) {
        this.log2Size = log2Size;
        this.bitsPerKey = bitsPerKey;
        this.sIndexBits = sIndexBits;
        this.tIndexBits = tIndexBits;

        int sBits = sKeyGenerator.getBits() - sIndexBits;
        int tBits = tKeyGenerator.getBits() - tIndexBits;
        sBytes = ByteUtil.getBytesCountByBitsCount(sBits);
        tBytes = ByteUtil.getBytesCountByBitsCount(tBits);

        sIndexBytes = ByteUtil.getBytesCountByBitsCount(sIndexBits);
        tIndexBytes = ByteUtil.getBytesCountByBitsCount(tIndexBits);
        sIndexMask = (1L << sIndexBits) - 1;
        tIndexMask = (1 << tIndexBits) - 1;

        System.out.println(sBits + " " + sBytes);
        System.out.println(tBits + " " + tBytes);
        System.out.println(sIndexBits + " " + sIndexBytes + " " + sIndexMask);
        System.out.println(tIndexBits + " " + tIndexBytes + " " + tIndexMask);
    }

    private byte[] getSKey(long s) {
        return ByteUtil.getKByte(s >> sIndexBits, sBytes);
    }

    private byte[] getSIndex(long s) {
        return ByteUtil.getKByte(s & sIndexMask, sIndexBytes);
    }

    private byte[] getTKey(int t) {
        return ByteUtil.getKByte(t >> tIndexBits, tBytes);
    }

    private byte[] getTIndex(int t) {
        return ByteUtil.getKByte(t & tIndexMask, tIndexBytes);
    }

    private BytesKey getSTIndex(long s, int t) {
        return new BytesKey(ByteUtil.concat(getSIndex(s), getTIndex(t)));
    }

    public IFilter getFilter(BytesKey stKey) {
        return filters.get(stKey);
    }

    public void insert(STObject stObject) {
        long s = sKeyGenerator.toNumber(stObject.getLocation());
        int t = tKeyGenerator.toNumber(stObject.getTime());

        BytesKey stIndex = getSTIndex(s, t);
        IFilter filter = getFilter(stIndex);
        if (filter == null) {
            filter = new InfiniFilter(log2Size, bitsPerKey);
            filters.put(stIndex, filter);
        }
        for (String keyword : stObject.getKeywords()) {
            filter.insert(ByteUtil.concat(kKeyGenerator.toBytes(keyword), getSKey(s), getTKey(t)));
        }
    }

    private boolean checkInFilter(IFilter filter, byte[] stKey, List<byte[]> kKeys, QueryType queryType) {
        if (filter == null) {
            return false;
        }
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : kKeys) {
                    if (filter.check(ByteUtil.concat(keyPre, stKey))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : kKeys) {
                    if (!filter.check(ByteUtil.concat(keyPre, stKey))) {
                        return false;
                    }
                }
                return true;
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
                    if (checkInFilter(getFilter(getSTIndex(s, t)), stKey, kKeys, queryType)) {
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
