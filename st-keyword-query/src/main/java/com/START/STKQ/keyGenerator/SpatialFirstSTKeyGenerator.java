package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.Constant;
import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.BytesKey;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.FilterManager.FilterManager;
import com.START.STKQ.util.FilterManager.QueueFilterManager;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.hash.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SpatialFirstSTKeyGenerator extends AbstractSTKeyGenerator {
    BloomFilter<byte[]> sBf;
    BloomFilter<byte[]> tBf;

    public SpatialFirstSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        super(spatialKeyGenerator, timeKeyGenerator);
    }

    public SpatialFirstSTKeyGenerator() {
        super();
    }

    public SpatialFirstSTKeyGenerator(BloomFilter<byte[]> sBf, BloomFilter<byte[]> tBf) {
        super();
        this.sBf = sBf;
        this.tBf = tBf;
    }

    public int getByteCount() {
        return timeKeyGenerator.getByteCount() + spatialKeyGenerator.getByteCount();
    }

    @Override
    public byte[] toKey(STObject object) {
        byte[] timeKey = timeKeyGenerator.toKey(object.getDate());
        byte[] spatialKey = spatialKeyGenerator.toKey(object.getLocation());
        byte[] objKey = ByteUtil.longToByte(object.getID());
        return ByteUtil.concat(spatialKey, timeKey, objKey);
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        ArrayList<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);
        for (Range<byte[]> spatialRange : spatialRanges) {
            long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow());
            long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh());
            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                byte[] now = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);
                for (Range<byte[]> timeRange : timeRanges) {
                    ranges.add(new Range<>(
                            ByteUtil.concat(now, timeRange.getLow()),
                            ByteUtil.concat(now, timeRange.getHigh())
                    ));
                }
            }
        }

        return ranges;
    }

    @Override
    public ArrayList<byte[]> toKeys(Query query, ArrayList<Range<byte[]>> sRanges, ArrayList<Range<byte[]>> tRanges) {
        ArrayList<byte[]> keys = new ArrayList<>();

//        ArrayList<String> keywords = query.getKeywords();
//        QueryType queryType = query.getQueryType();
//
//        if (sBf != null && tBf != null) {
//            ArrayList<byte[]> sCodes = new ArrayList<>();
//            ArrayList<byte[]> tCodes = new ArrayList<>();
//
//            for (Range<byte[]> spatialRange : sRanges) {
//                long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
//                long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
//
//                for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
//                    if (BfUtil.check(sBf, ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT), keywords, queryType)) {
//                        sCodes.add(ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT));
//                    }
//                }
//            }
//
//            int timeRangeStart = ByteUtil.toInt(tRanges.get(0).getLow()) >>> Constant.FILTER_ITEM_LEVEL;
//            int timeRangeEnd = ByteUtil.toInt(tRanges.get(0).getHigh()) >>> Constant.FILTER_ITEM_LEVEL;
//
//            for (int i = timeRangeStart; i <= timeRangeEnd; ++i) {
//                if (BfUtil.check(tBf, ByteUtil.getKByte(i, TIME_BYTE_COUNT), keywords, queryType)) {
//                    tCodes.add(ByteUtil.getKByte(i, TIME_BYTE_COUNT));
//                }
//            }
//
//            for (byte[] sCode : sCodes) {
//                for (byte[] tCode : tCodes) {
//                    keys.add(ByteUtil.concat(sCode, tCode));
//                }
//            }
//
//        } else {
//            for (Range<byte[]> spatialRange : sRanges) {
//                long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
//                long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
//                for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
//                    byte[] now = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);
//                    int timeRangeStart = ByteUtil.toInt(tRanges.get(0).getLow()) >>> Constant.FILTER_ITEM_LEVEL;
//                    int timeRangeEnd = ByteUtil.toInt(tRanges.get(0).getHigh()) >>> Constant.FILTER_ITEM_LEVEL;
//                    for (int j = timeRangeStart; j <= timeRangeEnd; ++j) {
//                        keys.add(ByteUtil.concat(now, ByteUtil.getKByte(j, TIME_BYTE_COUNT)));
//                    }
//                }
//            }
//        }

        LinkedList<Long> sKeys = new LinkedList<>();
        for (Range<byte[]> spatialRange : sRanges) {
            long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
            long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                if (sKeys.isEmpty() || i != sKeys.getLast()) {
                    sKeys.add(i);
                }
            }
        }

        int timeRangeStart = ByteUtil.toInt(tRanges.get(0).getLow()) >>> Constant.FILTER_ITEM_LEVEL;
        int timeRangeEnd = ByteUtil.toInt(tRanges.get(0).getHigh()) >>> Constant.FILTER_ITEM_LEVEL;
        int tKeySize = timeRangeEnd - timeRangeStart + 1;
        byte[][] timeKeys = new byte[tKeySize][TIME_BYTE_COUNT];

        int pos = 0;
        for (int j = timeRangeStart; j <= timeRangeEnd; ++j) {
            timeKeys[pos] = ByteUtil.getKByte(j, TIME_BYTE_COUNT);
            ++pos;
        }

        for (Long i : sKeys) {
            byte[] now = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);
//            int timeRangeStart = ByteUtil.toInt(tRanges.get(0).getLow()) >>> Constant.FILTER_ITEM_LEVEL;
//            int timeRangeEnd = ByteUtil.toInt(tRanges.get(0).getHigh()) >>> Constant.FILTER_ITEM_LEVEL;
//            for (int j = timeRangeStart; j <= timeRangeEnd; ++j) {
//                keys.add(ByteUtil.concat(now, ByteUtil.getKByte(j, TIME_BYTE_COUNT)));
//            }
            for (int j = 0; j < tKeySize; ++j) {
                keys.add(ByteUtil.concat(now, timeKeys[j]));
            }
        }

        return keys;
    }

    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (bloomFilter == null && !loadFilterDynamically) {
            return new ArrayList<>();
        }


        long begin;

//        begin = System.nanoTime();
        ArrayList<Range<byte[]>> sRanges = spatialKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> tRanges = timeKeyGenerator.toKeyRanges(query);
        
        List<Range<Long>> sRangesLong = sRanges.stream().map(
                key -> new Range<>(ByteUtil.toLong(key.getLow()), ByteUtil.toLong(key.getHigh()))).collect(Collectors.toList());
        Range<Integer> tRangesInt = new Range<>(
                ByteUtil.toInt(tRanges.get(0).getLow()), ByteUtil.toInt(tRanges.get(0).getHigh())
        );

        int sRangeSize = sRangesLong.size();

        ArrayList<byte[]> keysBefore = toKeys(query, sRanges, tRanges);

//        filterTime += System.nanoTime() - begin;

        QueryType queryType = query.getQueryType();

        ArrayList<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> filteredKeys;

        if (!loadFilterDynamically) {
            begin = System.nanoTime();
            filteredKeys = keysBefore.stream().parallel().filter(
                    key -> checkInBF(key, wordKeys, queryType)
            );
        } else {
            // get the filter id, since the keys are sorted, so we use list rather than set

            LinkedList<Long> sLongSet = new LinkedList<>();
            for (Range<Long> sRangeLong : sRangesLong) {
                long filterIdStart = sRangeLong.getLow() >>> (Constant.FILTER_LEVEL << 1);
                long filterIdEnd = sRangeLong.getHigh() >>> (Constant.FILTER_LEVEL << 1);
                for (long i = filterIdStart; i <= filterIdEnd; ++i) {
                    if (sLongSet.isEmpty() || sLongSet.getLast() != i) {
                        sLongSet.add(i);
                    }
                }
            }
            LinkedList<Integer> tIntSet = new LinkedList<>();
            int filterIdStart = tRangesInt.getLow() >>> Constant.FILTER_LEVEL;
            int filterIdEnd = tRangesInt.getHigh() >>> Constant.FILTER_LEVEL;
            for (int i = filterIdStart; i <= filterIdEnd; ++i) {
                tIntSet.add(i);
            }

            //13, -2, 5, -95, 1, -78, 3
            int needByteCountForS = Constant.SPATIAL_BYTE_COUNT - Constant.FILTER_LEVEL / 4;
            int needByteCountForT = Constant.TIME_BYTE_COUNT - Constant.FILTER_LEVEL / 8;
            Map<BytesKey, Filter> filters = new Hashtable<>();
            for (Long idS : sLongSet) {
                for (Integer idT : tIntSet) {
                    BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(idS, needByteCountForS), ByteUtil.getKByte(idT, needByteCountForT))));
                    Filter filter = null;
                    switch (flushStrategy) {
                        case HOTNESS: filter = FilterManager.getFilter(bfID); break;
                        case FIRST: filter = QueueFilterManager.getFilter(bfID); break;
                        case RANDOM: break;
                    }
                    if (filter != null) {
                        filters.put(bfID, filter);
                    }
                }
            }

            begin = System.nanoTime();
            filteredKeys = keysBefore.stream().parallel().filter(
                    key -> {
                        long sIDForBf = ByteUtil.toLong(Arrays.copyOfRange(key, 0, 4)) >>> ((Constant.FILTER_LEVEL - Constant.FILTER_ITEM_LEVEL) << 1);
                        int tIDForBf = ByteUtil.toInt(Arrays.copyOfRange(key, 4, 7)) >>> (Constant.FILTER_LEVEL - Constant.FILTER_ITEM_LEVEL);
                        BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, needByteCountForS), ByteUtil.getKByte(tIDForBf, needByteCountForT))));
                        return checkInFilter(key, wordKeys, queryType, filters.get(bfID));
                    }
            );
        }

        int shiftForS = Constant.FILTER_ITEM_LEVEL << 1;
        int shiftForT = Constant.FILTER_ITEM_LEVEL;
        int maxShiftForS = (1 << shiftForS) - 1;
        int maxShiftForT = (1 << shiftForT) - 1;

        filteredKeys = filteredKeys.parallel().map(
                key -> {
                    ArrayList<byte[]> keys = new ArrayList<>();
                    long sCode = ByteUtil.toLong(Arrays.copyOfRange(key, 0, 4));
                    int tCode = ByteUtil.toInt(Arrays.copyOfRange(key, 4, 7));
                    ArrayList<byte[]> sKeys = new ArrayList<>();
                    ArrayList<byte[]> tKeys = new ArrayList<>();

                    int pos = 0;
                    for (long i = sCode << shiftForS; i <= (sCode << shiftForS | maxShiftForS); ++i) {
                        for (int j = pos; j < sRangeSize; ++j) {
                            Range<Long> range = sRangesLong.get(j);
                            if (i >= range.getLow() && i <= range.getHigh()) {
                                sKeys.add(ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT));
                                pos = j;
                                break;
                            }
                        }
                    }

                    for (int i = tCode << shiftForT; i <= (tCode << shiftForT | maxShiftForT); ++i) {
                        if (i >= tRangesInt.getLow() && i <= tRangesInt.getHigh()) {
                            tKeys.add(ByteUtil.getKByte(i, TIME_BYTE_COUNT));
                        }
                    }

                    for (byte[] sKey : sKeys) {
                        for (byte[] tKey : tKeys) {
                            keys.add(ByteUtil.concat(sKey, tKey));
                        }
                    }

                    return keys;
                }
        ).flatMap(ArrayList::stream);

        ArrayList<Range<byte[]>> ranges = keysToRanges(filteredKeys);
        filterTime += System.nanoTime() - begin;
        return ranges;
//        return keysToRanges(filteredKeys);
    }
}