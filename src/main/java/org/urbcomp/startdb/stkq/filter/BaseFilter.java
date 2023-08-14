package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseFilter implements IFilter {

    public boolean check(byte[] key) { return true; }

    @Override
    public List<byte[]> filter(List<Range<Long>> sRanges, Range<Integer> tRange, List<String> keywords, QueryType queryType) {
        ArrayList<byte[]> result = new ArrayList<>();

        byte[][] wordsCode = keywords.stream().map(word -> ByteUtil.getKByte(word.hashCode(), 4)).toArray(byte[][]::new);

        int tStart = tRange.getLow();
        int tEnd = tRange.getHigh();
        for (Range<Long> spatialRange : sRanges) {
            long spatialRangeStart = spatialRange.getLow();
            long spatialRangeEnd = spatialRange.getHigh();

            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                for (int j = tStart; j <= tEnd; ++j) {
                    byte[] stKey = ByteUtil.concat(
                            ByteUtil.getKByte(i, 4),
                            ByteUtil.getKByte(j, 3)
                    );
                    switch (queryType) {
                        case CONTAIN_ONE:
                            for (byte[] wordCode : wordsCode) {
                                byte[] key = ByteUtil.concat(wordCode, stKey);
                                if (check(key)) {
                                    result.add(stKey);
                                    break;
                                }
                            }
                            break;
                        case CONTAIN_ALL:
                            boolean containAll = true;
                            for (byte[] wordCode : wordsCode) {
                                byte[] key = ByteUtil.concat(wordCode, stKey);
                                if (!check(key)) {
                                    containAll = false;
                                    break;
                                }
                            }
                            if (containAll) {
                                result.add(stKey);
                            }
                            break;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void insert(byte[] code) {}
}
