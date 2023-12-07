package org.urbcomp.startdb.stkq.stream;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.*;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;
import org.urbcomp.startdb.stkq.util.GeoUtil;

import java.util.*;

public class StreamQueryGenerator {
    private final static int QUERY_COUNT = 1000;
    private final static int[] COUNT;
    Random random = new Random();
    private final Map<Integer, List<byte[]>> stMap = new TreeMap<>(Comparator.reverseOrder());
    private final Map<BytesKey, Set<String>> st2Keywords = new HashMap<>();
    private final Set<String> keywords = new HashSet<>();
    ISpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
    TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();

    public List<Query> generatorQuery() {
        int timeCount = COUNT.length;
        if (stMap.size() < timeCount) {
            return new ArrayList<>();
        }
        List<Query> queries = new ArrayList<>();
        Iterator<Map.Entry<Integer, List<byte[]>>> it = stMap.entrySet().iterator();
        List<String> allKeywords = new ArrayList<>(keywords);

        while (timeCount-- > 0 && it.hasNext()) {
            Map.Entry<Integer, List<byte[]>> entry = it.next();
            Integer tInt = entry.getKey();
            byte[] tKey = tKeyGenerator.numberToBytes(tInt);
            List<byte[]> sKeys = entry.getValue();

            Date date = DateUtil.getDateAfterHours(tInt);
//            System.out.println(date);

            Collections.shuffle(sKeys);
            int sCount = COUNT[timeCount];
            for (int i = 0; i < sCount; ++i) {
                Query query = new Query();
                query.setQueryType(QueryType.CONTAIN_ONE);

                query.setEndTime(date);
                query.setStartTime(DateUtil.getDateAfterHours(date, new int[]{-1, -2, -3, -4}[random.nextInt(4)]));

                byte[] sKey = sKeys.get(Math.min(i, sKeys.size() - 1));
                Location loc = sKeyGenerator.bytesToPoint(sKey);
                MBR mbr = GeoUtil.getMBRByCircle(new Location(loc.getLat(), loc.getLon()), 2000);
                query.setMbr(mbr);

                ArrayList<String> qKeywords;

                if (random.nextDouble() < 0.5) {
                    qKeywords = getRandomKeywordsLimit(new ArrayList<>(st2Keywords.get(new BytesKey(ByteUtil.concat(tKey, sKey)))));
                } else {
                    qKeywords = getRandomKeywords(allKeywords);
                }
                query.setKeywords(qKeywords);

                queries.add(query);
            }
        }
        return queries;
    }

    public void insert(STObject cur) {
        byte[] sBytes = sKeyGenerator.toBytes(cur.getLocation());
        byte[] tBytes = tKeyGenerator.toBytes(cur.getTime());

        int tKeyInt = tKeyGenerator.toNumber(cur.getTime());
        BytesKey stKey = new BytesKey(ByteUtil.concat(tBytes, sBytes));

        st2Keywords.computeIfAbsent(stKey, k -> new HashSet<>()).addAll(cur.getKeywords());
        stMap.computeIfAbsent(tKeyInt, k -> new ArrayList<>()).add(sBytes);
        keywords.addAll(cur.getKeywords());
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(COUNT));
        System.out.println(Arrays.stream(COUNT).sum());
    }

    private ArrayList<String> getRandomKeywords(List<String> allKeywords) {
        int n = allKeywords.size();
        int m = random.nextInt(Math.min(n, 3)) + 1;
        Set<String> set = new HashSet<>();
        while (set.size() < m) {
            set.add(allKeywords.get(random.nextInt(n)));
        }
        return new ArrayList<>(set);
    }

    private ArrayList<String> getRandomKeywordsLimit(List<String> keywords) {
        ArrayList<String> keywords1 = new ArrayList<>(keywords);
        Collections.shuffle(keywords1);
        int n = keywords1.size();
        int m = random.nextInt(Math.min(n, 3)) + 1;
        if (m == n) {
            return keywords1;
        }
        return new ArrayList<>(keywords1.subList(0, m));
    }

    static {
        int n = 8;
        int[] a = new int[n];
        COUNT = new int[n];
        for (int i = 0; i < n; ++i) {
            a[i] = i + 1;
        }
        int sum = Arrays.stream(a).sum();
        for (int i = 0; i < n; ++i) {
            COUNT[i] = QUERY_COUNT * a[i] / sum;
        }
        int rev = QUERY_COUNT - Arrays.stream(COUNT).sum();
        COUNT[COUNT.length - 1] += rev;
    }
}
