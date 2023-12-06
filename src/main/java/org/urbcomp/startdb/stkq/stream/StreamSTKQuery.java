package org.urbcomp.startdb.stkq.stream;

import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class StreamSTKQuery {
    private final static String DIR = "/usr/data/tweetSorted.csv";
    private final static int TIME_BIN = 4;

    private static Date getDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    public static void main(String[] args) {
        Date win = null;
        StreamSTFilter filter = new StreamSTFilter(8, 4, new StreamLRUFilterManager(3, 13));
        StreamQueryGenerator queryGenerator = new StreamQueryGenerator();
        QueryProcessor processor = new QueryProcessor("testTweet", filter);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(DIR).toPath())))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();

                if (win == null) {
                    win = getDate(now);
                } else if (DateUtil.getHours(win, now) >= TIME_BIN) {

                    filter.doClear();
                    List<Query> queries = queryGenerator.generatorQuery();
                    System.out.println(win);
                    System.out.println(now);
                    for (Query query : queries) {
                        System.out.println(query);
                    }
//                    for (Query query : queries) {
//                        processor.getResult(query);
//                    }

                    while (DateUtil.getHours(win, now) >= TIME_BIN) {
                        win = DateUtil.getDateAfterHours(win, TIME_BIN);
                    }
                }
//                System.out.println(win + " " + now);

                // put into filter
                filter.insert(cur);
                queryGenerator.insert(cur);

                if (++count >= 100000) {
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
//        catch (ParseException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }
}
