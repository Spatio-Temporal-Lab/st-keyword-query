package com.START.STKQ.app;

import com.START.STKQ.model.Location;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.io.DataProcessor;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

class TimeCompare implements Comparable<TimeCompare> {
    int time1;
    int time2;
    double similarity;

    public TimeCompare(int t1, int t2, double similarity) {
        this.time1 = t1;
        this.time2 = t2;
        this.similarity = similarity;
    }
    @Override
    public int compareTo(TimeCompare o) {
        return -Double.compare(similarity, o.similarity);
    }

    public String toString() {
        return time1 + " " + time2 + " " + similarity;
    }
}

public class DataDistribution {

    private static int union(HashSet<String> h1, HashSet<String> h2) {
        HashSet<String> result = new HashSet<>();
        result.addAll(h1);
        result.addAll(h2);
        return result.size();
    }

    private static int intersects(HashSet<String> h1, HashSet<String> h2) {
        HashSet<String> result = new HashSet<>(h1);
        result.retainAll(h2);
        return result.size();
    }

    public static void main(String[] args) throws ParseException {

        DataProcessor dataProcessor = new DataProcessor();

        double minLat = 38.5;
        double minLon = -90.1;
        double maxLat = 38.6;
        double maxLon = -90;
        dataProcessor.setLocationRange(new Range<>(new Location(minLat, minLon), new Location(maxLat, maxLon)));
//        ArrayList<STObject> objects = dataReader.getSTObjects("E:\\data\\yelp.csv");
        ArrayList<STObject> objects = new ArrayList<>();
        for (int i = 1; i <= 4; ++i) {
            objects.addAll(dataProcessor.getSTObjects("E:\\data\\tweet\\" + "tweet_" + i + ".csv"));
        }

        // time distribution
        ArrayList<HashSet<String>> timeKeywords = new ArrayList<>();
        for (int i = 0; i < 24; ++i) {
            timeKeywords.add(new HashSet<>());
        }
        for (STObject object : objects) {
            timeKeywords.get(object.getDate().getHours()).addAll(object.getKeywords());
        }
        for (int i = 0; i < 24; ++i) {
            System.out.println(i + " to " + (i + 1) + ": " + timeKeywords.get(i).size());
        }
        ArrayList<TimeCompare> timeCompares = new ArrayList<>();
        ArrayList<ArrayList<TimeCompare>> timeComparesEachHour = new ArrayList<>();
        for (int i = 0; i < 24; ++i) {
            timeComparesEachHour.add(new ArrayList<>());
        }
        for (int i = 0; i < 24; ++i) {
            for (int j = 0; j < 24; ++j) {
                if (i != j) {
                    HashSet<String> keywordsI = timeKeywords.get(i);
                    HashSet<String> keywordsJ = timeKeywords.get(j);
                    int unionSize = union(keywordsI, keywordsJ);
                    int intersectsSize = intersects(keywordsI, keywordsJ);
                    TimeCompare timeCompare = new TimeCompare(i, j, (double) intersectsSize / unionSize);
                    if (j > i) {
                        timeCompares.add(timeCompare);
                    }
                    timeComparesEachHour.get(i).add(timeCompare);
                }
            }
        }
        Collections.sort(timeCompares);
        for (int i = 0; i < 24; ++i) {
            Collections.sort(timeComparesEachHour.get(i));
        }
        for (TimeCompare timeCompare : timeCompares) {
            System.out.println(timeCompare);
        }
        for (int i = 0; i < 24; ++i) {
            for (int j = 0; j < 3; ++j) {
                System.out.println(timeComparesEachHour.get(i).get(j));
            }
            System.out.println("------------------------");
        }

        // spatial distribution
        ArrayList<ArrayList<HashSet<String>>> spatialKeywords = new ArrayList<>();
        int cutCount = 10;
        double latPerCut = (maxLat - minLat) / cutCount;
        double lonPerCut = (maxLon - minLon) / cutCount;
        for (int i = 0; i < cutCount; ++i) {
            spatialKeywords.add(new ArrayList<>());
            for (int j = 0; j < cutCount; ++j) {
                spatialKeywords.get(i).add(new HashSet<>());
            }
        }
        for (STObject object : objects) {
            int id1 = (int) ((object.getLat() - minLat) / latPerCut);
            int id2 = (int) ((object.getLon() - minLon) / lonPerCut);
            spatialKeywords.get(Math.min(cutCount - 1, id1)).get(Math.min(cutCount - 1, id2)).addAll(object.getKeywords());
        }
        int minSize = Integer.MAX_VALUE;
        int maxSize = 0;
        for (int i = 0; i < cutCount; ++i) {
            for (int j = 0; j < cutCount; ++j) {
                int size = spatialKeywords.get(i).get(j).size();
                System.out.print(size + " ");
                if (size != 0) {
                    minSize = Math.min(minSize, size);
                }
                maxSize = Math.max(maxSize, size);
            }
            System.out.println();
        }
        System.out.println("min: " + minSize);
        System.out.println("max: " + maxSize);
    }
}
