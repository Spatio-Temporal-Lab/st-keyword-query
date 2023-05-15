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

    public ArrayList<byte[]> toKeys(Query query) { return new ArrayList<>(); }

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

    public boolean checkInBF(byte[] key, ArrayList<byte[]> keyPres, QueryType queryType, BloomFilter<byte[]> bloomFilter) {
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
//                    System.out.println("yyy" + Arrays.toString(ByteUtil.concat(keyPre, key)));
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
            while (j < n && keysLong.get(j) == keysLong.get(j - 1) + 1)
                ++j;
            ranges.add(new Range<>(
                    ByteUtil.getKByte(keysLong.get(i), SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT),
                    ByteUtil.getKByte(keysLong.get(j - 1), SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT)
            ));
            i = j;
        }

        return ranges;
    }

    public ArrayList<byte[]> toFilteredKeys(Query query) {
        ArrayList<byte[]> keysBefore = toKeys(query);

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> filteredKeys = keysBefore.stream().parallel().filter(
                key -> checkInBF(key, wordKeys, queryType)
        );
        return filteredKeys.collect(Collectors.toCollection(ArrayList::new));
    }

    public ArrayList<byte[]> toFilteredKeys(Query query, BloomFilter<byte[]> bloomFilter) {
        ArrayList<byte[]> keysBefore = toKeys(query);

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> filteredKeys = keysBefore.stream().parallel().filter(
                key -> checkInBF(key, wordKeys, queryType, bloomFilter)
        );

        return filteredKeys.collect(Collectors.toCollection(ArrayList::new));
    }

    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (bloomFilter == null) {
            return new ArrayList<>();
        }
        ArrayList<byte[]> keysBefore = toKeys(query);

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> filteredKeys = keysBefore.stream().parallel().filter(
                key -> checkInBF(key, wordKeys, queryType)
        );
        return keysToRanges(filteredKeys);
    }
}
