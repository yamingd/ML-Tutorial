package com.spark.demo.kmeans;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Created by yaming_deng on 14-3-27.
 */
public class MatchColPair implements Serializable {
    public int uid;
    public int targetId;
    public Map<Integer, Double> rates;

    public double sum(){
        double t = 0;
        Collection<Double> values = rates.values();
        for(Double d : values){
            t += d;
        }
        return t;
    }
}
