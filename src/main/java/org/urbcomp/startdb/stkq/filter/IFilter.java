package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public interface IFilter {
    // todo 这个方法需要放在query processor里面
    default List<byte[]> shrink(Query query, ISpatialKeyGenerator sKeyGenerator, TimeKeyGenerator tKeyGenerator,
                                KeywordKeyGenerator keywordGenerator) {
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);

        List<byte[]> result = new ArrayList<>();

        byte[][] wordsCode = query.getKeywords().stream()
                .map(keywordGenerator::toBytes).toArray(byte[][]::new);

        int tStart = tRange.getLow();
        int tEnd = tRange.getHigh();
        for (Range<Long> sRange : sRanges) {
            long sRangeStart = sRange.getLow();
            long sRangeEnd = sRange.getHigh();
            for (long i = sRangeStart; i <= sRangeEnd; ++i) {
                for (int j = tStart; j <= tEnd; ++j) {
                    byte[] stKey = ByteUtil.concat(sKeyGenerator.numberToBytes(i), tKeyGenerator.numberToBytes(j));
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
    boolean insert(byte[] code);
    boolean check(byte[] key);
    boolean sacrifice();

    int size();

    default void writeTo(ByteArrayOutputStream bos) {}
}
