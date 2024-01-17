package org.urbcomp.startdb.stkq.STILT;

import org.locationtech.geomesa.curve.NormalizedDimension;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class STILTIndex implements Closeable {
    private final STILT trie;
    private final static int DEFAULT_PRECISION = 14;
    private final NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(DEFAULT_PRECISION);
    private final NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(DEFAULT_PRECISION);
    private final TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator(3);
    private final int keywordMask = (1 << 16) - 1;

    public STILTIndex(byte treeHeight) {
        this.trie = new STILT(treeHeight);
    }

    public void insert(STObject object) {
        int time = timeKeyGenerator.toNumber(object.getTime());
        int lon = normalizedLon.normalize(object.getLon());
        int lat = normalizedLat.normalize(object.getLat());
        long id = object.getID();
        for (String s : object.getKeywords()) {
            trie.insert(crossEnc(lat, lon, s.hashCode() & keywordMask, time), id);
        }
        RedisIO.set(0, ByteUtil.longToBytes(id), object.toByteArray());
    }

    public List<STObject> getResult(Query query) {
        int minLat = normalizedLat.normalize(query.getMinLat());
        int maxLat = normalizedLat.normalize(query.getMaxLat());
        int minLon = normalizedLon.normalize(query.getMinLon());
        int maxLon = normalizedLon.normalize(query.getMaxLon());
        int minT = timeKeyGenerator.toNumber(query.getStartTime());
        int maxT = timeKeyGenerator.toNumber(query.getEndTime());

        Set<Long> idSet = new HashSet<>();
        for (String s : query.getKeywords()) {
            idSet.addAll(trie.query(new QueryBox(minLat, maxLat, minLon, maxLon, minT, maxT, s.hashCode() & keywordMask)));
        }

        List<byte[]> idList =  idSet.stream().map(ByteUtil::longToBytes).collect(Collectors.toList());
        return RedisIO.get(0, idList).stream().map(STObject::new).filter(o -> STKUtil.check(o, query)).collect(Collectors.toList());
    }

    private long crossEnc(int a, int b, int c, int d) {
        long result = 0;
        for (int i = 15; i >= 0; --i) {
            result |= (long) (a >> i & 1) << (i << 2 | 3);
            result |= (long) (b >> i & 1) << (i << 2 | 2);
            result |= (long) (c >> i & 1) << (i << 2 | 1);
            result |= (long) (d >> i & 1) << (i << 2);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        RedisIO.close();
    }
}
