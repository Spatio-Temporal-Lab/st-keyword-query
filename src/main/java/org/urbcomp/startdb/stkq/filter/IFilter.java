package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public interface IFilter {
    default List<byte[]> shrink(List<Range<Long>> sRanges, Range<Integer> tRange, List<String> keywords) {
        List<byte[]> result = new ArrayList<>();

        byte[][] wordsCode = keywords.stream()
                .map(word -> ByteUtil.getKByte(word.hashCode(), Constant.KEYWORD_BYTE_COUNT)).toArray(byte[][]::new);

        int tStart = tRange.getLow();
        int tEnd = tRange.getHigh();
        for (Range<Long> spatialRange : sRanges) {
            long spatialRangeStart = spatialRange.getLow();
            long spatialRangeEnd = spatialRange.getHigh();

            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                for (int j = tStart; j <= tEnd; ++j) {
                    byte[] stKey = ByteUtil.concat(
                            ByteUtil.getKByte(i, Constant.SPATIAL_BYTE_COUNT),
                            ByteUtil.getKByte(j, Constant.TIME_BYTE_COUNT)
                    );
                    for (byte[] wordCode : wordsCode) {
                        byte[] key = ByteUtil.concat(wordCode, stKey);
                        if (check(key)) {
                            result.add(stKey);
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }
    void insert(byte[] code);
    boolean check(byte[] key);

}
