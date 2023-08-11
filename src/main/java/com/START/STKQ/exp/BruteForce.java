package com.START.STKQ.exp;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;

import java.util.ArrayList;

public class BruteForce {
    public static ArrayList<STObject> getAnswer(ArrayList<STObject> objects, Query query) {
        ArrayList<STObject> result = new ArrayList<>();
        for (STObject object : objects) {
            if (!object.getLocation().in(query.getMBR())) {
                continue;
            }
            if (object.getDate().before(query.getS()) || object.getDate().after(query.getT())) {
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
