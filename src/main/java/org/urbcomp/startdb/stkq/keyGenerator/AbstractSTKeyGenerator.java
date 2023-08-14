package org.urbcomp.startdb.stkq.keyGenerator;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.hash.BloomFilter;
import org.urbcomp.startdb.stkq.constant.FilterType;
import org.urbcomp.startdb.stkq.constant.FlushStrategy;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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

    public boolean checkInFilter(byte[] key, List<byte[]> keyPres, QueryType queryType) {
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
        return new ArrayList<>();
    }
}
