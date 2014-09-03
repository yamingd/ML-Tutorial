package com.spark.demo.kmeans;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 *
 * 统计用户资料属性字段值的分布
 *
 * Created by yaming_deng on 14-3-26.
 */
public class PersonAttrProfile implements Serializable {

    public void start(String[] args){
        System.setProperty("spark.scheduler.mode", "FAIR");

        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]).setAppName("PersonAttrProfile:5w");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.cores.max", args[2]);
        conf.set("spark.executor.memory", args[3]);
        conf.set("spark.scheduler.allocation.file", "/usr/local/spark/spark/conf/fairscheduler.xml");
        conf.setSparkHome(System.getenv("SPARK_HOME"));
        conf.setJars(new String[]{System.getenv("SPARK_APP_JAR")});

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLocalProperty("spark.scheduler.pool", "production");

        this.start(sc);
    }

    public void start(JavaSparkContext sc) {
        String hdfsUrl = "hdfs://10.10.9.156:9000/tmp/eharmony_spark/user_data_5w.txt";
        System.out.println(hdfsUrl);

        JavaRDD<String> file = sc.textFile(hdfsUrl);

        JavaRDD<Integer[]> person = file.map(new Function<String, Integer[]>() {
            @Override
            public Integer[] call(String s) throws Exception {
                String[] temp = s.split("\t");
                Integer[] item = new Integer[temp.length];
                for (int i = 0; i < item.length; i++) {
                    item[i] = Integer.parseInt(temp[i]);
                }
                return item;
            }
        });

        long size = file.count();
        int personPerPartion = 500;
        int pfactor = (int) size / personPerPartion;

        JavaPairRDD<String, Integer> sdd = person.repartition(pfactor).flatMap(new PairFlatMapFunction<Integer[], String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Integer[] item) throws Exception {
                List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>(item.length);
                for (int i = 1; i < item.length; i++) {
                    String key = i + "\t" + item[i];
                    results.add(new Tuple2<String, Integer>(key, 1));
                }
                return results;
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        }).coalesce(1, true).sortByKey(true).cache();

        final Broadcast<Long> brc = sc.broadcast(size);

        sdd.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> tuple2) throws Exception {
                return String.format("%s\t%s\t%s", tuple2._1, tuple2._2, ((float)tuple2._2) / brc.value());
            }
        }).saveAsTextFile(hdfsUrl + "_profile");

    }

}
