package com.START.STKQ.filter;

import com.START.STKQ.constant.Constant;
import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;

import java.util.ArrayList;

public class TRosetta extends BasicRosetta {

    public TRosetta(int n) {
        super(n);
    }

    public void insert(byte[] pre, int t) {
        for (int i = 0; i < n; ++i) {
            filters.get(i).insert(ByteUtil.concat(pre, ByteUtil.getKByte(t >> (n - i - 1), Constant.TIME_BYTE_COUNT)), false);
        }
    }

    public ArrayList<Range<byte[]>> filter(ArrayList<Range<Long>> sRanges, Range<Integer> tRange, ArrayList<byte[]> keyPres, QueryType queryType) {
        int tLow = tRange.getLow();
        int tHigh = tRange.getHigh();
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();
        for (Range<Long> sRange : sRanges) {
            long sLow = sRange.getLow();
            long sHigh = sRange.getHigh();
            for (long s = sLow; s <= sHigh; ++s) {
                ranges.addAll(filter(ByteUtil.getKByte(s, Constant.SPATIAL_BYTE_COUNT), tLow, tHigh, keyPres, queryType));
            }
        }
        return ranges;
    }

    public ArrayList<Range<byte[]>> toRanges(byte[] s, ArrayList<Integer> a) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        int n = a.size();

        for (int i = 0; i < n; ) {
            int j = i + 1;
            while (j < n && a.get(j) <= a.get(j - 1) + 1)
                ++j;
            ranges.add(new Range<>(
                    ByteUtil.concat(s, ByteUtil.getKByte(a.get(i), Constant.TIME_BYTE_COUNT)),
                    ByteUtil.concat(s, ByteUtil.getKByte(a.get(j - 1), Constant.TIME_BYTE_COUNT))
            ));
            i = j;
        }

        return ranges;
    }

    public boolean checkInFilter(ChainedInfiniFilter filter, byte[] s, int t, ArrayList<byte[]> keyPres, QueryType queryType) {
        if (filter == null) {
            return false;
        }

        byte[] key = ByteUtil.concat(s, ByteUtil.getKByte(t, Constant.TIME_BYTE_COUNT));
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (filter.search(ByteUtil.concat(keyPre, key))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!filter.search(ByteUtil.concat(keyPre, key))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    public ArrayList<Range<byte[]>> filter(byte[] s, int tLow, int tHigh, ArrayList<byte[]> keyPres, QueryType queryType) {
        int low = tLow >> (n - 1);
        int high = tHigh >> (n - 1);

        ChainedInfiniFilter filter = filters.get(0);
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = low; i <= high; ++i) {
            if (checkInFilter(filter, s, i, keyPres, queryType)) {
                result.add(i);
            }
        }

        if (n == 1) {
            return toRanges(s, result);
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

        return toRanges(s, result);
    }
}
