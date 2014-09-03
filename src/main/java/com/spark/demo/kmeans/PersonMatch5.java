package com.spark.demo.kmeans;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yaming_deng on 14-3-26.
 */
public class PersonMatch5 implements Serializable {

    public void start(String[] args){
        System.setProperty("spark.scheduler.mode", "FAIR");

        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]).setAppName("PersonMatch:5-5w");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.cores.max", args[2]);
        conf.set("spark.executor.memory", args[3]);
        conf.set("spark.scheduler.allocation.file", "/usr/local/spark/spark/conf/fairscheduler.xml");
        conf.setSparkHome(System.getenv("SPARK_HOME"));
        conf.setJars(new String[]{System.getenv("SPARK_APP_JAR")});

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLocalProperty("spark.scheduler.pool", "production");

        this.start(sc, "1");
    }

    public void start(JavaSparkContext sc, String fid) {

        //String hdfsUrl = "hdfs://10.10.10.16:9000/tmp/eharmony/data";
        //String hdfsUrl = "hdfs://10.10.10.16:9000/tmp/eharmony_spark/user_col_" + fid;
        String hdfsUrl = "hdfs://10.10.10.16:9000/tmp/eharmony_spark/user_data.txt";

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
        }).cache();

        long size = file.count();
        int pfactor = (int) size / 10000;
        pfactor = pfactor / 2 * pfactor;
        if (pfactor == 0){
            pfactor = 1;
        }

        //System.out.println("Load All Datas, size = " + size);

        System.out.println("build person.flatMap");

        final Broadcast<List<Integer[]>> brc = sc.broadcast(person.collect());
        final Accumulator<Integer> acc = sc.accumulator(0);
        final Accumulator<Integer> pCount = sc.accumulator(0);

        person.repartition(pfactor * 200).mapPartitions(new FlatMapFunction<Iterator<Integer[]>, String>() {
            @Override
            public Iterable<String> call(Iterator<Integer[]> items) throws Exception {
                pCount.add(1);
                final int batchSize = 250;
                final List<Integer[]> cands = brc.value();
                final int size = cands.size();
                final int poolSize = size / batchSize / 2;
                final ExecutorService es = Executors.newFixedThreadPool(poolSize);
                Date st1 = new Date();
                int count = 0;
                final List<String> results = Collections.synchronizedList(new ArrayList(size));
                while (items.hasNext()) {
                    final Integer[] item = items.next();
                    System.out.println(item[0]);
                    for (int i = 0; i < size; ) {
                        final int start = i;
                        final int end = start + batchSize;
                        es.submit(new Runnable() {
                            @Override
                            public void run() {
                                for (int j = start; j < end && j < size; j++) {
                                    Integer[] row = cands.get(j);
                                    double sum = 0.0;
                                    for (int k = 1; k < row.length; k++) {
                                        sum += Math.abs(item[k] - row[k]);
                                    }
                                    //System.out.println("mapping " + item[0] + ", " + row[0] + " ==> " + sum);
                                    String str = String.format("%s\t%s\t%s", item[0], row[0], sum);
                                    results.add(str);
                                }
                            }
                        });
                        i = end;
                    }
                    count++;
                    acc.add(1);
                }
                //System.out.println("start matching count=" + count);
                // Wait for threads to finish off.
                es.shutdown();
                // Wait for everything to finish.
                while (!es.isTerminated()) {
                    //System.out.println("wait for shut down executor");
                    Thread.sleep(5000);
                }

                long ts = new Date().getTime() - st1.getTime();
                System.out.println("start matching DONE. ts=" + ts + ", count=" + count + ", size=" + size);
                return results;
            }
        }).saveAsTextFile(hdfsUrl + "_result");

        System.out.println("Person = " + acc.value());
        System.out.println("Partition Count = " + pCount.value());
    }

    public static void main(String[] args){
        int size = 10*1000;
        int batchSize = 1000;
        for (int i = 0; i < size; ) {
            final int start = i;
            final int end = start + batchSize;
            System.out.println(start+":"+end);
            i = end;
        }
    }
}
