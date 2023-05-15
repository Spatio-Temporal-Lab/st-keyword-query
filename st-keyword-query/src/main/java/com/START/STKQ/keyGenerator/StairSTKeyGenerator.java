package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import com.github.StairSketch.StairBf;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.stream.Stream;

public class StairSTKeyGenerator extends SpatialFirstSTKeyGenerator {

    private final StairBf stairBf;

    public StairSTKeyGenerator(StairBf bf) {
        stairBf = bf;
    }

    public boolean checkInBF(byte[] key, ArrayList<byte[]> keyPres, QueryType queryType, int l, int r) {
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (stairBf.query(ByteUtil.concat(keyPre, key), l, r)) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!stairBf.query(ByteUtil.concat(keyPre, key), l, r)) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    @Override
    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (stairBf == null) {
            return new ArrayList<>();
        }
        ArrayList<byte[]> keysBefore = toKeys(query);
        Stream<byte[]> lastKeyStream = keysBefore.stream();

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        int l = timeKeyGenerator.getNumber(query.getS());
        int r = timeKeyGenerator.getNumber(query.getT());
        Stream<byte[]> keyStream = lastKeyStream.parallel().filter(key -> checkInBF(key, wordKeys, queryType, l, r));

        return keysToRanges(keyStream);
    }
}