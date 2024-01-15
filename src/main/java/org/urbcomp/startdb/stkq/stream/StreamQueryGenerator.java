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
    private final QueryDistributionEnum queryDistribution;
    private final QueryType queryType;
    private final int totalQueryCount;          // 生成的总的查询个数
    private final static int preHours = 24;        // 可能查最近的小时数
    private final static int[] timeRangeHour = new int[]{2}; //new int[]{1, 2, 3, 4, 5};  // 可能生成的查询时间范围小时数
    private final static int[] spatialRangeMeter = new int[]{2000}; // new int[]{100, 300, 500, 1000, 2000, 4000};         // 可能生成的查询空间范围米数
    private final static int[] keywordNumber = new int[]{3};    // 查询中关键字的个数
    private final Random random = new Random(0);
    private final Map<Integer, List<byte[]>> tKey2sKeys = new TreeMap<>(Comparator.reverseOrder());
    private final Map<BytesKey, Set<String>> stKey2Keywords = new HashMap<>();
    private final Set<String> keywords = new HashSet<>();
    private final ISpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
    private final TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();

    public StreamQueryGenerator(int count, QueryDistributionEnum queryDistribution, QueryType queryType) {
        this.totalQueryCount = count;
        this.queryDistribution = queryDistribution;
        this.queryType = queryType;
    }

    public List<Query> generateQuery() {
        int[] endTimeHourCount = getEndTimeHourCount(Math.min(preHours, tKey2sKeys.size()));       // 查询结束时间落在某个小时内的查询数目
        List<Query> queries = new ArrayList<>();
        Iterator<Map.Entry<Integer, List<byte[]>>> it = tKey2sKeys.entrySet().iterator();
        List<String> allKeywords = new ArrayList<>(keywords);

        int timeCount = endTimeHourCount.length;
        while (timeCount-- > 0 && it.hasNext()) {
            Map.Entry<Integer, List<byte[]>> entry = it.next();

            Integer tInt = entry.getKey();
            byte[] tKey = tKeyGenerator.numberToBytes(tInt);
            Date endTime = DateUtil.getDateAfterHours(tInt + 1);   // 加1是为了保证当前小时能够查询到

            List<byte[]> sKeys = entry.getValue();
            Collections.shuffle(sKeys, random);

            int sCount = endTimeHourCount[timeCount];
            for (int i = 0; i < sCount; ++i) {
                Query query = new Query();
                query.setQueryType(this.queryType);
                query.setEndTime(endTime);
                query.setStartTime(DateUtil.getDateAfterHours(endTime, -timeRangeHour[random.nextInt(timeRangeHour.length)]));

                byte[] sKey = sKeys.get(random.nextInt(sKeys.size() - 1));
                Location loc = sKeyGenerator.bytesToPoint(sKey);
                MBR mbr = GeoUtil.getMBRByCircle(new Location(loc.getLat(), loc.getLon()), spatialRangeMeter[random.nextInt(spatialRangeMeter.length)] / 2.0);
                query.setMbr(mbr);

                List<String> qKeywords;
                // 一半肯定有结果，一半可能有结果
                int kwNumber = keywordNumber[random.nextInt(keywordNumber.length)];
                if (random.nextDouble() < 0.5) {
                    BytesKey stKey = new BytesKey(ByteUtil.concat(tKey, sKey));
                    qKeywords = getRandomKeywordsLimit(new ArrayList<>(stKey2Keywords.get(stKey)), kwNumber);
                } else {
                    qKeywords = getRandomKeywords(allKeywords, kwNumber);
                }
                query.setKeywords(qKeywords);

                queries.add(query);
            }
        }
        Collections.shuffle(queries, random);
        return queries;
    }

    public void insert(STObject cur) {
        byte[] sBytes = sKeyGenerator.toBytes(cur.getLocation());
        byte[] tBytes = tKeyGenerator.toBytes(cur.getTime());
        int tKeyInt = tKeyGenerator.toNumber(cur.getTime());
        BytesKey stKey = new BytesKey(ByteUtil.concat(tBytes, sBytes));

        stKey2Keywords.computeIfAbsent(stKey, k -> new HashSet<>()).addAll(cur.getKeywords());
        tKey2sKeys.computeIfAbsent(tKeyInt, k -> new ArrayList<>()).add(sBytes);
        keywords.addAll(cur.getKeywords());
    }

    private int[] getEndTimeHourCount(int preHours) {
        int[] endTimeHourCount = new int[preHours];
        int[] distribution = new int[preHours];
        switch (queryDistribution) {
            case LINEAR:
                distribution[0] = 1;
                for (int i = 1; i < distribution.length; i++) {
                    distribution[i] = distribution[i - 1] + 1;
                }
                break;
            case UNIFORM:
                Arrays.fill(distribution, 1);
                break;
            case GEOMETRIC:
                distribution[0] = 1;
                for (int i = 1; i < distribution.length; i++) {
                    distribution[i] = distribution[i - 1] * 2;
                }
                break;
            default:
                throw new RuntimeException("Not Implemented!");
        }
        int sum = Arrays.stream(distribution).sum();
        for (int i = 0; i < endTimeHourCount.length; i++) {
            endTimeHourCount[i] = totalQueryCount * distribution[i] / sum;
        }
        int rest = totalQueryCount - Arrays.stream(endTimeHourCount).sum(); // 由于取整的原因，还有部分未分配
        endTimeHourCount[endTimeHourCount.length - 1] += rest;
        return endTimeHourCount;
    }

    private List<String> getRandomKeywords(List<String> allKeywords, int kwNumber) {
        int n = allKeywords.size();
        if (n <= kwNumber) {
            return new ArrayList<>(allKeywords);
        }
        Set<String> set = new HashSet<>();
        while (set.size() < kwNumber) {
            set.add(allKeywords.get(random.nextInt(n)));
        }
        return new ArrayList<>(set);
    }

    private List<String> getRandomKeywordsLimit(List<String> keywords, int kwNumber) {
        List<String> kw = new ArrayList<>(keywords);
        Collections.shuffle(kw, random);
        if (kwNumber >= kw.size()) {
            return kw;
        }
        return new ArrayList<>(kw.subList(0, kwNumber));
    }
}
