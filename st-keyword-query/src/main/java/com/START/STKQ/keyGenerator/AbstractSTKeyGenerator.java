package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.google.common.hash.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractSTKeyGenerator implements IKeyGenerator<STObject>, Serializable {
    protected final SpatialKeyGenerator spatialKeyGenerator;
    protected final TimeKeyGenerator timeKeyGenerator;
    protected final int TIME_BYTE_COUNT = 3;
    protected final int SPATIAL_BYTE_COUNT = 4;

    protected BloomFilter<byte[]> bloomFilter;
    private static final long serialVersionUID = 6529685098267757693L;

    public AbstractSTKeyGenerator() {
        spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        timeKeyGenerator = new TimeKeyGenerator();
    }

    public AbstractSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        this.spatialKeyGenerator = spatialKeyGenerator;
        this.timeKeyGenerator = timeKeyGenerator;
    }

    public void setBloomFilter(BloomFilter<byte[]> bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public SpatialKeyGenerator getSpatialKeyGenerator() {
        return spatialKeyGenerator;
    }

    public TimeKeyGenerator getTimeKeyGenerator() {
        return timeKeyGenerator;
    }

    public int getByteCount() {
        return 0;
    }

    @Override
    public byte[] toKey(STObject object) {
        return new byte[]{0};
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        return new ArrayList<>();
    }

    public ArrayList<byte[]> toKeys(Query query, ArrayList<Range<byte[]>> sRange, ArrayList<Range<byte[]>> tRange) {return new ArrayList<>(); }

    public boolean checkInBF(byte[] key, ArrayList<byte[]> keyPres, QueryType queryType) {
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (bloomFilter.mightContain(ByteUtil.concat(keyPre, key))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!bloomFilter.mightContain(ByteUtil.concat(keyPre, key))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    public ArrayList<Range<byte[]>> keysToRanges(Stream<byte[]> keys) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();
        List<Long> keysLong = keys.map(ByteUtil::toLong).sorted().collect(Collectors.toList());

        int n = keysLong.size();

        for (int i = 0; i < n; ) {
            int j = i + 1;
            while (j < n && keysLong.get(j) <= keysLong.get(j - 1) + 1)
                ++j;
            ranges.add(new Range<>(
                    ByteUtil.getKByte(keysLong.get(i), SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT),
                    ByteUtil.getKByte(keysLong.get(j - 1), SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT)
            ));
            i = j;
        }

        return ranges;
    }

    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (bloomFilter == null) {
            return new ArrayList<>();
        }

        ArrayList<Range<byte[]>> sRanges = spatialKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> tRanges = timeKeyGenerator.toKeyRanges(query);

        List<Range<Long>> sRangesLong = sRanges.stream().map(
                key -> new Range<>(ByteUtil.toLong(key.getLow()), ByteUtil.toLong(key.getHigh()))).collect(Collectors.toList());
        Range<Integer> tRangesInt = new Range<>(
                ByteUtil.toInt(tRanges.get(0).getLow()), ByteUtil.toInt(tRanges.get(0).getHigh())
        );

        ArrayList<byte[]> keysBefore = toKeys(query, sRanges, tRanges);

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> filteredKeys = keysBefore.stream().parallel().filter(
                key -> checkInBF(key, wordKeys, queryType)
        );

        filteredKeys = filteredKeys.parallel().map(
                key -> {
                    ArrayList<byte[]> keys = new ArrayList<>();
                    long sCode = ByteUtil.toLong(Arrays.copyOfRange(key, 0, 4));
                    int tCode = ByteUtil.toInt(Arrays.copyOfRange(key, 4, 7));
                    ArrayList<byte[]> sKeys = new ArrayList<>();
                    ArrayList<byte[]> tKeys = new ArrayList<>();

                    for (long i = sCode << 4; i <= (sCode << 4 | 15); ++i) {
                        for (Range<Long> sRange : sRangesLong) {
                            if (i >= sRange.getLow() && i <= sRange.getHigh()) {
                                sKeys.add(ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT));
                                break;
                            }
                        }
                    }

                    for (int i = tCode << 2; i <= (tCode << 2 | 3); ++i) {
                        if (i >= tRangesInt.getLow() && i <= tRangesInt.getHigh()) {
                            tKeys.add(ByteUtil.getKByte(i, TIME_BYTE_COUNT));
                        }
                    }

                    for (byte[] sKey : sKeys) {
                        for (byte[] tKey : tKeys) {
                            keys.add(ByteUtil.concat(sKey, tKey));
                        }
                    }
                    return keys;
                }
        ).flatMap(ArrayList::stream);

        return keysToRanges(filteredKeys);
    }
}
