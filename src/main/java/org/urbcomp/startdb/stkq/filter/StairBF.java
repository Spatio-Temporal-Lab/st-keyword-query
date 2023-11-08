package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.BloomFilter;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class StairBF {
    private final int level;
    private final int minT;
    private final int maxT;
    private final BloomFilter[][] bfs;
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

    public List<byte[]> shrink(Query query, ISpatialKeyGeneratorNew sKeyGenerator, TimeKeyGeneratorNew tKeyGenerator, KeywordKeyGeneratorNew keywordGenerator) {
        List<Range<Long>> sRanges = sKeyGenerator.toNumberRanges(query);
        Range<Integer> tRange = tKeyGenerator.toNumberRanges(query).get(0);

        List<byte[]> result = new ArrayList<>();

        byte[][] wordsCode = query.getKeywords().stream()
                .map(keywordGenerator::toBytes).toArray(byte[][]::new);

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
}
