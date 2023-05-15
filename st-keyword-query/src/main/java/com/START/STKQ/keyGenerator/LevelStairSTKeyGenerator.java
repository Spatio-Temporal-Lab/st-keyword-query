package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import com.github.StairSketch.StairBf;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

public class LevelStairSTKeyGenerator extends SpatialFirstSTKeyGenerator {
    int level;
    StairBf[] bfs;

    public LevelStairSTKeyGenerator(int start, int level, int stairLevel, int bitsPerBf, int maxSize) {
        this.level = level;
        bfs = new StairBf[level];
        for (int i = level - 1; i >= 0; --i) {
            maxSize >>= 1;
            bfs[i] = new StairBf(start, stairLevel, bitsPerBf);
        }
    }

    public LevelStairSTKeyGenerator(int level, StairBf[] bloomFilters) {
        this.level = level;
        this.bfs = bloomFilters;
    }

    public byte[] getKeyInLevel(byte[] key, int level) {
        // level - 1: 0
        // level - 2: 1
        // 0 : level - 1
        long s = ByteUtil.toLong(Arrays.copyOf(key, SPATIAL_BYTE_COUNT));
        return ByteUtil.concat(
                ByteUtil.getKByte(s >> ((this.level - 1 - level) << 1), SPATIAL_BYTE_COUNT),
                Arrays.copyOfRange(key, SPATIAL_BYTE_COUNT, SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT)
        );
    }

    public boolean checkInBF(byte[] key, ArrayList<byte[]> keyPres, QueryType queryType, int level, int l, int r) {
        StairBf sbf = bfs[level];
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (sbf.query(ByteUtil.concat(keyPre, getKeyInLevel(key, level)), l, r)) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!sbf.query(ByteUtil.concat(keyPre, getKeyInLevel(key, level)), l, r)) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    @Override
    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (bfs == null) {
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
        int l = timeKeyGenerator.getNumber(query.getS());
        int r = timeKeyGenerator.getNumber(query.getT());
        for (int i = 0; i < level; ++i) {
            int finalI = i;
            keyStream = lastKeyStream.parallel().filter(
                    key -> checkInBF(key, wordKeys, queryType, finalI, l, r));
            lastKeyStream = keyStream;
        }
        return keysToRanges(keyStream);
    }
}