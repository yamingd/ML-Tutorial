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
 * 统计用户资料属性字段值关联的分布
 *
 * Created by yaming_deng on 14-3-26.
 */
public class PersonAttrPairProfile implements Serializable {

    public void start(String[] args){
        System.setProperty("spark.scheduler.mode", "FAIR");

        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]).setAppName("PersonAttrPairProfile:5w");
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
        int personPerPartion = 50;
        int pfactor = (int) size / personPerPartion;

        JavaPairRDD<String, Integer> sdd = person.repartition(pfactor).mapPartitions(new PairFlatMapFunction<Iterator<Integer[]>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterator<Integer[]> itor) throws Exception {
                Map<String, Integer> map = new HashMap<String, Integer>();
                Integer[] pairs = new Integer[]{0, 6, 7, 8};
                while (itor.hasNext()) {
                    Integer[] item = itor.next();
                    for(int i=1; i<pairs.length; i++){
                        String key = String.format("%s\t%s\t%s\t%s", i, pairs[i], item[i], item[pairs[i]]);
                        if (map.containsKey(key)){
                            map.put(key, map.get(key) + 1);
                        }else{
                            map.put(key, 1);
                        }
                    }
                }
                List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>(map.size());
                Iterator<String> vitor = map.keySet().iterator();
                while(vitor.hasNext()){
                    String key = vitor.next();
                    results.add(new Tuple2<String, Integer>(key, map.get(key)));
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
        }).saveAsTextFile(hdfsUrl + "_pair2_profile");

    }

}
