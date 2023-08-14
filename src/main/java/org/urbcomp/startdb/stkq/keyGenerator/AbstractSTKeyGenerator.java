package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.constant.FilterType;
import org.urbcomp.startdb.stkq.constant.FlushStrategy;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.FilterManager.FilterManager;
import org.urbcomp.startdb.stkq.util.FilterManager.QueueFilterManager;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.hash.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractSTKeyGenerator implements IKeyGenerator<STObject>, Serializable {
    protected final SpatialKeyGenerator spatialKeyGenerator;
    protected final TimeKeyGenerator timeKeyGenerator;
    protected final int TIME_BYTE_COUNT = 3;
    protected final int SPATIAL_BYTE_COUNT = 4;

    @SuppressWarnings("all")
    protected BloomFilter<byte[]> bloomFilter;

    protected ChainedInfiniFilter filter;
    protected FilterType filterType = FilterType.DYNAMIC;

    protected FlushStrategy flushStrategy;
    private static final long serialVersionUID = 6529685098267757693L;
    protected long filterTime = 0;

    public long getFilterTime() {
        return filterTime;
    }

    public AbstractSTKeyGenerator() {
        spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        timeKeyGenerator = new TimeKeyGenerator();
    }

    public AbstractSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        this.spatialKeyGenerator = spatialKeyGenerator;
        this.timeKeyGenerator = timeKeyGenerator;
    }

    public void setFilter(ChainedInfiniFilter filter) {
        this.filter = filter;
    }

    /**
     * todo 这个为什么返回0？
     *
     * @return 0
     */
    public int getByteCount() {
        return 0;
    }

    /**
     * todo 为什么返回空字节？
     */
    @Override
    public byte[] toKey(STObject object) {
        return new byte[]{0};
    }

    /**
     * todo 为什么返回空字节？
     */
    @Override
    public List<Range<byte[]>> toKeyRanges(Query query) {
        return new ArrayList<>();
    }

    protected List<byte[]> toKeys(Query query, List<Range<byte[]>> sRange, List<Range<byte[]>> tRange) {
        return new ArrayList<>();
    }

    @SuppressWarnings("all")
    public boolean checkInBF(byte[] key, List<byte[]> keyPres, QueryType queryType) {
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (bloomFilter.mightContain(ByteUtil.concat(keyPre, key))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!bloomFilter.mightContain(ByteUtil.concat(keyPre, key))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }

    public boolean checkInFilter(byte[] key, List<byte[]> keyPres, QueryType queryType) throws IOException, ClassNotFoundException {
        //4 byte for spatial key and 3 byte for time key
        long sIDForBf = ByteUtil.toLong(Arrays.copyOfRange(key, 0, 4)) >>> 16;
        int tIDForBf = ByteUtil.toInt(Arrays.copyOfRange(key, 4, 7)) >>> 8;
        BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, 2), ByteUtil.getKByte(tIDForBf, 2))));

        Filter filter = null;
        switch (flushStrategy) {
            case HOTNESS:
                filter = FilterManager.getFilter(bfID);
                break;
            case FIRST:
                filter = QueueFilterManager.getFilter(bfID);
                break;
            case RANDOM:
                break;
        }
        if (filter == null) {
            return false;
        }

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

    public boolean checkInFilter(byte[] key, List<byte[]> keyPres, QueryType queryType, Filter filter) {
        if (filter == null) {
            return false;
        }

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

    public ArrayList<Range<byte[]>> keysToRanges(Stream<byte[]> keys) {

        ArrayList<Range<byte[]>> ranges = new ArrayList<>();
        List<Long> keysLong = keys.map(ByteUtil::toLong).sorted().collect(Collectors.toList());

        int n = keysLong.size();

        for (int i = 0; i < n; ) {
            int j = i + 1;
            while (j < n && keysLong.get(j) <= keysLong.get(j - 1) + 1)
                ++j;
            ranges.add(new Range<>(
                    ByteUtil.getKByte(keysLong.get(i), SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT),
                    ByteUtil.getKByte(keysLong.get(j - 1), SPATIAL_BYTE_COUNT + TIME_BYTE_COUNT)
            ));
            i = j;
        }

        return ranges;
    }

    public List<Range<byte[]>> toFilteredKeyRanges(Query query) {
        if (bloomFilter == null && filterType.equals(FilterType.BLOOM)) {
            return new ArrayList<>();
        }

        List<Range<byte[]>> sRanges = spatialKeyGenerator.toKeyRanges(query);
        List<Range<byte[]>> tRanges = timeKeyGenerator.toKeyRanges(query);

        List<Range<Long>> sRangesLong = sRanges.stream().map(
                key -> new Range<>(ByteUtil.toLong(key.getLow()), ByteUtil.toLong(key.getHigh()))).collect(Collectors.toList());
        Range<Integer> tRangesInt = new Range<>(
                ByteUtil.toInt(tRanges.get(0).getLow()), ByteUtil.toInt(tRanges.get(0).getHigh())
        );

        List<byte[]> keysBefore = toKeys(query, sRanges, tRanges);

        QueryType queryType = query.getQueryType();

        List<byte[]> wordKeys = new ArrayList<>();
        for (String s : query.getKeywords()) {
            wordKeys.add(Bytes.toBytes(s.hashCode()));
        }

        Stream<byte[]> filteredKeys;
        if (filterType.equals(FilterType.BLOOM)) {
            filteredKeys = keysBefore.stream().parallel().filter(
                    key -> checkInBF(key, wordKeys, queryType)
            );
        } else {
            filteredKeys = keysBefore.stream()
                    .parallel()
                    .filter(
                            key -> {
                                try {
                                    return checkInFilter(key, wordKeys, queryType);
                                } catch (IOException | ClassNotFoundException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );
        }

        filteredKeys = filteredKeys
                .parallel()
                .map(
                        key -> {
                            ArrayList<byte[]> keys = new ArrayList<>();
                            long sCode = ByteUtil.toLong(Arrays.copyOfRange(key, 0, 4));
                            int tCode = ByteUtil.toInt(Arrays.copyOfRange(key, 4, 7));
                            ArrayList<byte[]> sKeys = new ArrayList<>();
                            ArrayList<byte[]> tKeys = new ArrayList<>();

                            for (long i = sCode << 4; i <= (sCode << 4 | 15); ++i) {
                                for (Range<Long> sRange : sRangesLong) {
                                    if (i >= sRange.getLow() && i <= sRange.getHigh()) {
                                        sKeys.add(ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT));
                                        break;
                                    }
                                }
                            }

                            for (int i = tCode << 2; i <= (tCode << 2 | 3); ++i) {
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

        return keysToRanges(filteredKeys);
    }
}
