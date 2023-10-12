package org.urbcomp.startdb.stkq.filter;

import org.apache.lucene.util.RamUsageEstimator;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSTFilter {
    private final int sBytes;
    private final int tBytes;
    protected final int sBits;
    protected final int tBits;
    protected final long sMask;
    protected final int tMask;
    protected final int sIndexBytes;
    protected final int tIndexBytes;

    protected final ISpatialKeyGeneratorNew sKeyGenerator = new HilbertSpatialKeyGeneratorNew();
    protected final TimeKeyGeneratorNew tKeyGenerator = new TimeKeyGeneratorNew();
    protected final KeywordKeyGeneratorNew kKeyGenerator = new KeywordKeyGeneratorNew();

    public AbstractSTFilter(int sBits, int tBits) {
        this.sBits = sBits;
        this.tBits = tBits;

        int sIndexBits = sKeyGenerator.getBits() - sBits;
        int tIndexBits = tKeyGenerator.getBits() - tBits;

        sBytes = ByteUtil.getBytesCountByBitsCount(sBits);
        tBytes = ByteUtil.getBytesCountByBitsCount(tBits);
        sIndexBytes = ByteUtil.getBytesCountByBitsCount(sIndexBits);
        tIndexBytes = ByteUtil.getBytesCountByBitsCount(tIndexBits);

        sMask = (1L << sBits) - 1;
        tMask = (1 << tBits) - 1;
    }

    byte[] getSKey(long s) {
        return ByteUtil.getKByte(s & sMask, sBytes);
    }

    private byte[] getSIndex(long s) {
        return ByteUtil.getKByte(s >> sBits, sIndexBytes);
    }

    byte[] getTKey(int t) {
        return ByteUtil.getKByte(t & tMask, tBytes);
    }

    private byte[] getTIndex(int t) {
        return ByteUtil.getKByte(t >> tBits, tIndexBytes);
    }

    BytesKey getSTIndex(long s, int t) {
        return new BytesKey(ByteUtil.concat(getSIndex(s), getTIndex(t)));
    }

    protected boolean checkInFilter(IFilter filter, byte[] stKey, List<byte[]> kKeys, QueryType queryType) {
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

    public void insert(STObject stObject) {
    }

    public boolean check(STObject stObject) {
        return false;
    }

    public List<byte[]> shrink(Query query) {
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();

        List<byte[]> results = new ArrayList<>();

        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();
            for (long s = sLow; s <= sHigh; ++s) {
                for (int t = tLow; t <= tHigh; ++t) {
                    results.add(ByteUtil.concat(
                            sKeyGenerator.numberToBytes(s),
                            tKeyGenerator.numberToBytes(t)
                    ));
                }
            }
        }

        return results;
    }

    public long size() { return RamUsageEstimator.sizeOf(this); }

    public void out() throws IOException {}

    public void in() {}

    public List<byte[]> shrinkWithIO(Query query) throws IOException {
        return null;
    }
}
