package org.urbcomp.startdb.stkq.exp;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;

import java.util.ArrayList;

public class BruteForce {
    public static ArrayList<STObject> getAnswer(ArrayList<STObject> objects, Query query) {
        ArrayList<STObject> result = new ArrayList<>();
        for (STObject object : objects) {
            if (!object.getLocation().in(query.getMBR())) {
                continue;
            }
            if (object.getDate().before(query.getStartTime()) || object.getDate().after(query.getEndTime())) {
                continue;
            }
            if (query.getQueryType().equals(QueryType.CONTAIN_ALL)) {
                boolean flag = true;
                for (String s : query.getKeywords()) {
                    if (!object.getKeywords().contains(s)) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    result.add(object);
                }
            } else if (query.getQueryType().equals(QueryType.CONTAIN_ONE)) {
                boolean flag = false;
                for (String s : query.getKeywords()) {
                    if (object.getKeywords().contains(s)) {
                        flag = true;
                        break;
                    }
                }
                if (flag) {
                    result.add(object);
                }
            }
        }
        return result;
    }
}
