package org.urbcomp.startdb.stkq.filter;

import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractSTFilter implements ISTKFilter {
    private final int sBytes;
    private final int tBytes;
    protected final int sBits;
    protected final int tBits;
    protected final long sMask;
    protected final int tMask;
    protected final int sIndexBytes;
    protected final int tIndexBytes;

    protected final ISpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
    protected final TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();
    protected final KeywordKeyGenerator kKeyGenerator = new KeywordKeyGenerator();

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

    protected byte[] getSKey(long s) {
        return ByteUtil.getKByte(s & sMask, sBytes);
    }

    private byte[] getSIndex(long s) {
        return ByteUtil.getKByte(s >> sBits, sIndexBytes);
    }

    protected byte[] getTKey(int t) {
        return ByteUtil.getKByte(t & tMask, tBytes);
    }

    private byte[] getTIndex(int t) {
        return ByteUtil.getKByte(t >> tBits, tIndexBytes);
    }

    protected BytesKey getSTIndex(long s, int t) {
        return new BytesKey(ByteUtil.concat(getSIndex(s), getTIndex(t)));
    }

    protected boolean checkInFilter(IFilter filter, byte[] stKey, List<byte[]> kKeys, QueryType queryType) {
        if (filter == null) {
            return false;
        }
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : kKeys) {
                    byte[] key = ByteUtil.concat(keyPre, stKey);
                    if (filter.check(key)) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : kKeys) {
                    byte[] key = ByteUtil.concat(keyPre, stKey);
                    if (!filter.check(key)) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    protected boolean checkInFilter(IFilter filter, Set<BytesKey> fnSet, byte[] stKey, List<byte[]> kKeys, QueryType queryType) {
        if (filter == null) {
            return false;
        }
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : kKeys) {
                    byte[] key = ByteUtil.concat(keyPre, stKey);
                    if (filter.check(key) || fnSet.contains(new BytesKey(key))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : kKeys) {
                    byte[] key = ByteUtil.concat(keyPre, stKey);
                    if (!filter.check(key) && !fnSet.contains(new BytesKey(key))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    protected List<Range<byte[]>> merge(List<Long> keysLong) {
        keysLong.sort(Comparator.naturalOrder());
        int mask = (1 << tKeyGenerator.getBits()) - 1;
        List<Range<Long>> temp = new ArrayList<>();
        for (long keyLong : keysLong) {
            if (temp.isEmpty()) {
                temp.add(new Range<>(keyLong, keyLong));
            } else {
                Range<Long> last = temp.get(temp.size() - 1);
                if (last.getHigh() + 1 >= keyLong) {
                    last.setHigh(keyLong);
                } else {
                    temp.add(new Range<>(keyLong, keyLong));
                }
            }
        }

        return temp.stream().map(
                rl -> {
                    byte[] sKey = sKeyGenerator.numberToBytes(rl.getLow() >> tKeyGenerator.getBits());
                    int tLow_ = (int) (rl.getLow() & mask);
                    int thigh_ = (int) (rl.getHigh() & mask);

                    return new Range<>(
                            ByteUtil.concat(sKey, tKeyGenerator.numberToBytes(tLow_)),
                            ByteUtil.concat(sKey, tKeyGenerator.numberToBytes(thigh_))
                    );
                }
        ).collect(Collectors.toList());
    }

    public void insert(STObject stObject) throws IOException {
    }

    public List<byte[]> shrink(Query query) throws IOException {
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

    public List<Range<byte[]>> shrinkAndMerge(Query query) throws IOException {
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();

        int tIndexMin = tLow >> tBits;
        int tIndexMax = tHigh >> tBits;

        List<Range<byte[]>> results;
        QueryType queryType = query.getQueryType();
        List<byte[]> kKeys = query.getKeywords().stream().map(kKeyGenerator::toBytes).collect(Collectors.toList());

        List<Long> keysLong = new ArrayList<>();

        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();

            long sIndexMin = sLow >> sBits;
            long sIndexMax = sHigh >> sBits;

            for (long sIndex = sIndexMin; sIndex <= sIndexMax; ++sIndex) {
                for (int tIndex = tIndexMin; tIndex <= tIndexMax; ++tIndex) {
                    byte[] stIndex = ByteUtil.concat(ByteUtil.getKByte(sIndex, sIndexBytes), ByteUtil.getKByte(tIndex, tIndexBytes));
                    IFilter filter = getWithIO(stIndex);

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
                                keysLong.add(s << tKeyGenerator.getBits() | t);
                            }
                        }
                    }

                }
            }
        }

        keysLong.sort(Comparator.naturalOrder());
        int mask = (1 << tKeyGenerator.getBits()) - 1;
        List<Range<Long>> temp = new ArrayList<>();
        for (long keyLong : keysLong) {
            if (temp.isEmpty()) {
                temp.add(new Range<>(keyLong, keyLong));
            } else {
                Range<Long> last = temp.get(temp.size() - 1);
                if (last.getHigh() + 1 >= keyLong) {
                    last.setHigh(keyLong);
                } else {
                    temp.add(new Range<>(keyLong, keyLong));
                }
            }
        }

        results = temp.stream().map(
                rl -> {
                    byte[] sKey = sKeyGenerator.numberToBytes(rl.getLow() >> tKeyGenerator.getBits());
                    int tLow_ = (int) (rl.getLow() & mask);
                    int thigh_ = (int) (rl.getHigh() & mask);

                    return new Range<>(
                            ByteUtil.concat(sKey, tKeyGenerator.numberToBytes(tLow_)),
                            ByteUtil.concat(sKey, tKeyGenerator.numberToBytes(thigh_))
                    );
                }
        ).collect(Collectors.toList());

        return results;
    }

    public IFilter getWithIO(byte[] stIndex) {
        return null;
    }

    public long ramUsage() { return RamUsageEstimator.sizeOf(this); }

    public void out() {}

    public List<byte[]> shrinkWithIO(Query query) {
        return null;
    }
}
