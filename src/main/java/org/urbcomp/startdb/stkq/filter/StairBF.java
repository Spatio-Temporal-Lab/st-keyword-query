package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.BloomFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StairBF implements ISTKFilter {
    private int level;
    private int minT;
    private int maxT;
    private BloomFilter[][] bfs;
    protected final ISpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
    protected final TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();
    protected final KeywordKeyGenerator kKeyGenerator = new KeywordKeyGenerator();
    /*
    * [l,mid),[mid,r]
    * */
    public StairBF(int level, int numEntries, int bitsPerEntry, int minT, int maxT) {
        this.level = level;
        this.minT = minT;
        this.maxT = maxT;

        bfs = new BloomFilter[level][];
        bfs[0] = new BloomFilter[1];
        bfs[0][0] = new BloomFilter(numEntries, bitsPerEntry, 0);
        for (int i = 1; i < level; ++i) {
            bfs[i] = new BloomFilter[2];
            numEntries *= 2;
            for (int j = 0; j < 2; ++j) {
                bfs[i][j] = new BloomFilter(numEntries, bitsPerEntry, i);
            }
        }
    }

    public void insert(byte[] code, int t) {
        int minNow = minT;
        int maxNow = maxT;
        int mid = (minNow + maxNow) / 2;
        if (t < mid) {
            bfs[level - 1][0].insert(code, t, false, level - 1);
            return;
        } else {
            bfs[level - 1][1].insert(code, t, false, level - 1);
            minNow = mid;
        }
        int i = level - 2;
        for (; i > 0; --i) {
            mid = (minNow + maxNow) / 2;
            if (t < mid) {
                bfs[i][0].insert(code, t, false, i);
                return;
            } else {
                bfs[i][1].insert(code, t, false, i);
                minNow = mid;
            }
        }
        if (i == 0) {
            bfs[0][0].insert(code, t, false, 0);
        }
    }

    public void insert(STObject object) {
        byte[] sBytes = sKeyGenerator.toBytes(object.getLocation());
        int t = tKeyGenerator.toNumber(object.getTime());
        byte[] tBytes = tKeyGenerator.numberToBytes(t);
        for (String keyword : object.getKeywords()) {
            insert(ByteUtil.concat(kKeyGenerator.toBytes(keyword), sBytes, tBytes), t);
        }
    }

    private boolean intersect(int l, int r, int ll, int rr) {
        return !(r < ll || l > rr);
    }

    private boolean intersect(int l, int r, int t) {
        return t >= l && t <= r;
    }

    public List<Integer> shrink(byte[] code, int qL, int qR) {
        List<Integer> result = new ArrayList<>();
        for (int i = qL; i <= qR; ++i) {
            if (query(code, i)) {
                result.add(i);
            }
        }
        return result;
    }

    public boolean query(byte[] code, int qL, int qR) {
        return query(code, level - 1, minT, maxT, qL, qR);
    }

    public boolean query(byte[] code, int t) {
        return query(code, level - 1, minT, maxT, t);
    }

    public boolean query(byte[] code, int level, int tL, int tR, int qL, int qR) {
        if (!intersect(tL, tR, qL, qR)) {
            return false;
        }
        if (level == 0) {
            return bfs[0][0].search(code, Math.max(tL, qL), Math.min(tR, qR), 0);
        }
        int mid = (tL + tR) / 2;
        if (intersect(tL, mid - 1, qL, qR)) {
            if (bfs[level][0].search(code, Math.max(tL, qL), Math.min(mid - 1, qR), level)) {
                return true;
            }
        } if (intersect(mid, tR, qL, qR)) {
            if (!bfs[level][1].search(code, Math.max(mid, qL), Math.min(tR, qR), level)) {
                return false;
            } else {
                return query(code, level - 1, mid, tR, qL, qR);
            }
        }
        return false;
    }

    public boolean query(byte[] code, int level, int tL, int tR, int q) {
        if (!intersect(tL, tR, q)) {
            return false;
        }
        if (level == 0) {
            return bfs[0][0].search(code, q, 0);
        }
        int mid = (tL + tR) / 2;
        if (q < mid) {
            return bfs[level][0].search(code, q, level);
        } else  {
            if (!bfs[level][1].search(code, q, level)) {
                return false;
            } else {
                return query(code, level - 1, mid, tR, q);
            }
        }
    }

    public List<byte[]> shrink(Query query) {
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);

        List<byte[]> result = new ArrayList<>();

        byte[][] wordsCode = query.getKeywords().stream()
                .map(kKeyGenerator::toBytes).toArray(byte[][]::new);

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
                        if (query(key, j)) {
                            result.add(stKey);
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }

    public List<Range<byte[]>> shrinkAndMerge(Query query) {
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);

        List<Range<byte[]>> results = new ArrayList<>();

        byte[][] wordsCode = query.getKeywords().stream()
                .map(kKeyGenerator::toBytes).toArray(byte[][]::new);

        int tStart = tRange.getLow();
        int tEnd = tRange.getHigh();

        ArrayList<Long> keysLong = new ArrayList<>();
        for (Range<Long> sRange : sRanges) {
            long sRangeStart = sRange.getLow();
            long sRangeEnd = sRange.getHigh();
            for (long i = sRangeStart; i <= sRangeEnd; ++i) {
                for (int j = tStart; j <= tEnd; ++j) {
                    byte[] stKey = ByteUtil.concat(sKeyGenerator.numberToBytes(i), tKeyGenerator.numberToBytes(j));
                    for (byte[] wordCode : wordsCode) {
                        byte[] key = ByteUtil.concat(wordCode, stKey);
                        if (query(key, j)) {
                            keysLong.add(i << tKeyGenerator.getBits() | j);
                            break;
                        }
                    }
                }
            }
        }

        keysLong.sort(Comparator.naturalOrder());
        int mask = (1 << tKeyGenerator.getBits()) - 1;
        List<Range<Long>> temp = new ArrayList<>();
        for (long keyLong : keysLong) {
            if (temp.isEmpty()) {
                temp.add(new Range<>(keyLong, keyLong));
            } else {
                Range<Long> last = temp.get(temp.size() - 1);
                if (last.getHigh() + 1 >= keyLong) {
                    last.setHigh(keyLong);
                } else {
                    temp.add(new Range<>(keyLong, keyLong));
                }
            }
        }

        results = temp.stream().map(
                rl -> {
                    byte[] sKey = sKeyGenerator.numberToBytes(rl.getLow() >> tKeyGenerator.getBits());
                    int tLow_ = (int) (rl.getLow() & mask);
                    int thigh_ = (int) (rl.getHigh() & mask);

                    return new Range<>(
                            ByteUtil.concat(sKey, tKeyGenerator.numberToBytes(tLow_)),
                            ByteUtil.concat(sKey, tKeyGenerator.numberToBytes(thigh_))
                    );
                }
        ).collect(Collectors.toList());

        return results;
    }

    public long size() {
        return RamUsageEstimator.sizeOf(this);
    }

    public void out() {
        RedisIO.set(3, "level".getBytes(), ByteUtil.getKByte(level, 4));
        RedisIO.set(3, "minT".getBytes(), ByteUtil.getKByte(minT, 4));
        RedisIO.set(3, "maxT".getBytes(), ByteUtil.getKByte(maxT, 4));
        int id = 0;
        for (BloomFilter[] bfs_ : bfs) {
            for (BloomFilter bf : bfs_) {
                try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                    bf.writeTo(os);
                    RedisIO.set(3, ByteUtil.getKByte(id, 4), os.toByteArray());
                    ++id;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void init() {
        level = ByteUtil.toInt(RedisIO.get(3, "level".getBytes()));
        minT = ByteUtil.toInt(RedisIO.get(3, "minT".getBytes()));
        maxT = ByteUtil.toInt(RedisIO.get(3, "maxT".getBytes()));

        bfs = new BloomFilter[level][];
        bfs[0] = new BloomFilter[1];

        BloomFilter temp = new BloomFilter();
        bfs[0][0] = temp.read(new ByteArrayInputStream(RedisIO.get(3, ByteUtil.getKByte(0, 4))));
        for (int i = 1; i < level; ++i) {
            bfs[i] = new BloomFilter[2];
            for (int j = 0; j < 2; ++j) {
                bfs[i][j] = temp.read(new ByteArrayInputStream(RedisIO.get(3, ByteUtil.getKByte(i * 2 - 1 + j, 4))));
            }
        }
    }
}
