package com.START.STKQ.filter;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;

import java.util.ArrayList;

public abstract class BaseFilter implements IFilter {

    public boolean check(byte[] key) { return true; }

    @Override
    public ArrayList<byte[]> filter(ArrayList<Range<Long>> sRanges, Range<Integer> tRange, ArrayList<String> keywords, QueryType queryType) {
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
