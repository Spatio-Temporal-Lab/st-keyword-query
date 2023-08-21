package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TRosetta extends BasicRosetta implements IRangeFilter {

    public TRosetta(int n) {
        super(n);
    }

    public void insert(byte[] pre, int t) {
        for (int i = 0; i < n; ++i) {
            filters.get(i).insert(ByteUtil.concat(pre, ByteUtil.getKByte(t >> (n - i - 1), Constant.TIME_BYTE_COUNT)), false);
        }
    }

    public boolean checkInFilter(ChainedInfiniFilter filter, byte[] s, int t, List<byte[]> kKeys, QueryType queryType) {
        if (filter == null) {
            return false;
        }

        byte[] key = ByteUtil.concat(s, ByteUtil.getKByte(t, Constant.TIME_BYTE_COUNT));
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : kKeys) {
                    if (filter.search(ByteUtil.concat(keyPre, key))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : kKeys) {
                    if (!filter.search(ByteUtil.concat(keyPre, key))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    public List<byte[]> shrink(byte[] s, int tLow, int tHigh, List<byte[]> keyPres, QueryType queryType) {
        int low = tLow >> (n - 1);
        int high = tHigh >> (n - 1);

        ChainedInfiniFilter filter = filters.get(0);
        List<Integer> result = new ArrayList<>();
        for (int i = low; i <= high; ++i) {
            if (checkInFilter(filter, s, i, keyPres, queryType)) {
                result.add(i);
            }
        }

        if (n == 1) {
            return result.stream().map(i -> ByteUtil.concat(s, tKeyGenerator.numberToBytes(i))).collect(Collectors.toList());
        }

        for (int i = 1; i < n; ++i) {
            int shift = n - i - 1;

            ArrayList<Integer> temp = new ArrayList<>();
            filter = filters.get(i);

            low = tLow >> shift;
            high = tHigh >> shift;

            for (int j : result) {
                int left = j << 1;
                if (left >= low) {
                    if (checkInFilter(filter, s, left, keyPres, queryType)) {
                        temp.add(left);
                    }
                }
                int right = left | 1;
                if (right <= high) {
                    if (checkInFilter(filter, s, right, keyPres, queryType)) {
                        temp.add(right);
                    }
                }
            }

            if (temp.size() == 0) {
                return new ArrayList<>();
            }
            result = new ArrayList<>(temp);
        }

        return result.stream().map(i -> ByteUtil.concat(s, tKeyGenerator.numberToBytes(i))).collect(Collectors.toList());
    }

    @Override
    public void insert(STObject object) {
        int t = tKeyGenerator.toNumber(object.getTime());
        byte[] sKey = sKeyGenerator.toBytes(object.getLocation());
        for (String k : object.getKeywords()) {
            byte[] kKey = kKeyGenerator.toBytes(k);
            insert(ByteUtil.concat(kKey, sKey), t);
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
            for (long s = sLow; s <= sHigh; ++s) {
                results.addAll(shrink(ByteUtil.getKByte(s, Constant.SPATIAL_BYTE_COUNT), tLow, tHigh, kKeys, queryType));
            }
        }

        return results;
    }
}
