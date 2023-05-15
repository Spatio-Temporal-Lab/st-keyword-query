package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SLevelSTKeyGenerator extends SpatialFirstSTKeyGenerator {
    int level;
    BloomFilter<byte[]>[] bloomFilters;

    public SLevelSTKeyGenerator(int level, int maxSize) {
        this.level = level;
        bloomFilters = new BloomFilter[level];
        for (int i = level - 1; i >= 0; --i) {
            maxSize >>= 1;
            bloomFilters[i] = BloomFilter.create(Funnels.byteArrayFunnel(), maxSize, 0.001);
        }
    }

    public SLevelSTKeyGenerator(int level, BloomFilter<byte[]>[] bloomFilters) {
        this.level = level;
        this.bloomFilters = bloomFilters;
    }

    public byte[] getKeyInLevel(byte[] key, int level) {
        // level - 1: 0
        // level - 2: 1
        // 0 : level - 1
        long s = ByteUtil.toLong(Arrays.copyOf(key, SPATIAL_BYTE_COUNT));
        return ByteUtil.concat(
                ByteUtil.getKByte(s >>> ((this.level - 1 - level) << 1), SPATIAL_BYTE_COUNT),
                Arrays.copyOfRange(key, SPATIAL_BYTE_COUNT, SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT)
        );
    }

    public boolean checkInBF(byte[] key, ArrayList<byte[]> keyPres, QueryType queryType, int level) {
        BloomFilter<byte[]> bloomFilter = bloomFilters[level];
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (bloomFilter.mightContain(ByteUtil.concat(keyPre, getKeyInLevel(key, level)))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!bloomFilter.mightContain(ByteUtil.concat(keyPre, getKeyInLevel(key, level)))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    @Override
    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (bloomFilters == null) {
            return new ArrayList<>();
        }
        ArrayList<byte[]> keysBefore = toKeys(query);
        Stream<byte[]> lastKeyStream = keysBefore.stream();

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> keyStream = keysBefore.stream();
        for (int i = 0; i < level; ++i) {
            int finalI = i;
            keyStream = lastKeyStream.parallel().filter(
                    key -> checkInBF(key, wordKeys, queryType, finalI));
            lastKeyStream = keyStream;
        }
        return keysToRanges(keyStream);
    }
}