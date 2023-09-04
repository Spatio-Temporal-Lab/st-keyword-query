package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SRosetta extends BasicRosetta implements IRangeFilter {

    public SRosetta(int n) {
        super(n);
    }

    private void insert(byte[] pre, long s) {
        for (int i = 0; i < n; ++i) {
            filters.get(i).insert(ByteUtil.concat(pre, sKeyGenerator.numberToBytes(s >> ((n - i - 1) << 1))), false);
        }
    }

    private List<byte[]> shrink(byte[] tKey, long sLow, long sHigh, List<byte[]> keyPres, QueryType queryType) {
        long low = sLow >> ((n - 1) << 1);
        long high = sHigh >> ((n - 1) << 1);

        ChainedInfiniFilter filter = filters.get(0);
        List<Long> result = new ArrayList<>();
        for (long i = low; i <= high; ++i) {
            if (checkInFilter(filter, ByteUtil.concat(tKey, sKeyGenerator.numberToBytes(i)), keyPres, queryType)) {
                result.add(i);
            }
        }

        for (int i = 1; i < n; ++i) {
            int shift = n - i - 1;

            List<Long> temp = new ArrayList<>();
            filter = filters.get(i);

            low = sLow >> (shift << 1);
            high = sHigh >> (shift << 1);

            for (long j : result) {
                long left = Math.max(j << 2, low);
                long right = Math.min(j << 2 | 3, high);
                for (long ss = left; ss <= right; ++ss) {
                    if (checkInFilter(filter, ByteUtil.concat(tKey, sKeyGenerator.numberToBytes(ss)), keyPres, queryType)) {
                        temp.add(ss);
                    }
                }
            }

            if (temp.size() == 0) {
                return new ArrayList<>();
            }
            result = temp;
        }

        return result.stream().map(i -> ByteUtil.concat(sKeyGenerator.numberToBytes(i), tKey)).collect(Collectors.toList());
    }

    @Override
    public void insert(STObject object) {
        byte[] tKey = tKeyGenerator.toBytes(object.getTime());
        long s = sKeyGenerator.toNumber(object.getLocation());
        for (String k : object.getKeywords()) {
            byte[] kKey = kKeyGenerator.toBytes(k);
            insert(ByteUtil.concat(kKey, tKey), s);
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

        for (int i = tLow; i <= tHigh; ++i) {
            byte[] tKey = tKeyGenerator.numberToBytes(i);
            for (Range<Long> sRange : sRanges) {
                long sLow = sRange.getLow();
                long sHigh = sRange.getHigh();
                results.addAll(shrink(tKey, sLow, sHigh, kKeys, queryType));
            }
        }

        return results;
    }
}
