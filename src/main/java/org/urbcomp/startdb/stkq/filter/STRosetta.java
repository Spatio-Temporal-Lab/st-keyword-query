package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.commons.math3.util.Pair;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class STRosetta extends BasicRosetta implements IRangeFilter {

    public STRosetta(int n) {
        super(n);
    }

    private void insert(byte[] pre, long s, int t) {
        for (int i = 0; i < n; ++i) {
            filters.get(i).insert(ByteUtil.concat(pre,
                    sKeyGenerator.numberToBytes(s >> ((n - i - 1) << 1)),
                    tKeyGenerator.numberToBytes(t >> (n - i - 1))), false);
        }
    }

    private List<byte[]> shrink(long sLow, long sHigh, int tLow, int tHigh, List<byte[]> keyPres, QueryType queryType) {
        int tLowThisLevel = tLow >> (n - 1);
        int tHighThisLevel = tHigh >> (n - 1);
        long sLowThisLevel = sLow >> ((n - 1) << 1);
        long sHighThisLevel = sHigh >> ((n - 1) << 1);

        ChainedInfiniFilter filter = filters.get(0);
        List<Pair<Long, Integer>> result = new ArrayList<>();

        for (long i = sLowThisLevel; i <= sHighThisLevel; ++i) {
            for (int j = tLowThisLevel; j <= tHighThisLevel; ++j) {
                if (checkInFilter(filter, ByteUtil.concat(sKeyGenerator.numberToBytes(i), tKeyGenerator.numberToBytes(j)), keyPres, queryType)) {
                    result.add(new Pair<>(i, j));
                }
            }
        }

        for (int level = 1; level < n; ++level) {
            int shift = n - level - 1;

            List<Pair<Long, Integer>> temp = new ArrayList<>();
            filter = filters.get(level);

            tLowThisLevel = tLow >> shift;
            tHighThisLevel = tHigh >> shift;
            sLowThisLevel = sLow >> (shift << 1);
            sHighThisLevel = sHigh >> (shift << 1);

            for (Pair<Long, Integer> stPair : result) {
                long s = stPair.getKey();
                int t = stPair.getValue();
                for (long i = Math.max(s << 2, sLowThisLevel); i <= Math.min(s << 2 | 3, sHighThisLevel); ++i) {
                    for (int j = Math.max(t << 1, tLowThisLevel); j <= Math.min(t << 1 | 1, tHighThisLevel); ++j) {
                        if (checkInFilter(filter, ByteUtil.concat(sKeyGenerator.numberToBytes(i), tKeyGenerator.numberToBytes(j)), keyPres, queryType)) {
                            temp.add(new Pair<>(i, j));
                        }
                    }
                }
            }

            if (temp.size() == 0) {
                return new ArrayList<>();
            }
            result = temp;
        }

        return result.stream().map(stPair -> ByteUtil.concat(sKeyGenerator.numberToBytes(stPair.getKey()),
                tKeyGenerator.numberToBytes(stPair.getValue()))).collect(Collectors.toList());
    }

    @Override
    public void insert(STObject object) {
        int t = tKeyGenerator.toNumber(object.getTime());
        long s = sKeyGenerator.toNumber(object.getLocation());
        for (String k : object.getKeywords()) {
            insert(kKeyGenerator.toBytes(k), s, t);
        }
    }

    @Override
    public List<byte[]> shrink(Query query) {
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();

        List<byte[]> results = new ArrayList<>();
        QueryType queryType = query.getQueryType();
        List<byte[]> kKeys = query.getKeywords().stream().map(k -> kKeyGenerator.toBytes(k)).collect(Collectors.toList());

        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();
            results.addAll(shrink(sLow, sHigh, tLow, tHigh, kKeys, queryType));
        }

        return results;
    }
}
