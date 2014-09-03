package com.spark.demo.kmeans;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yaming_deng on 14-3-26.
 */
public class PersonMatch implements Serializable {

    static final Logger logger = LogManager.getLogger(PersonMatch.class.getName());

    public void start(String[] args){
        System.setProperty("spark.scheduler.mode", "FAIR");

        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]).setAppName("PersonMatch:1");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.cores.max", "3");
        conf.set("spark.scheduler.allocation.file", "/usr/local/spark/spark/conf/fairscheduler.xml");
        conf.setSparkHome(System.getenv("SPARK_HOME"));
        conf.setJars(new String[]{System.getenv("SPARK_APP_JAR")});

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLocalProperty("spark.scheduler.pool", "production");

        this.start(sc, "1");
    }

    public void start(JavaSparkContext sc, String fid) {

        //String hdfsUrl = "hdfs://10.10.10.16:9000/tmp/eharmony/data";
        String hdfsUrl = "hdfs://10.10.10.16:9000/tmp/eharmony_spark/user_col_" + fid;

        System.out.println(hdfsUrl);

        JavaRDD<String> file = sc.textFile(hdfsUrl);

        JavaRDD<Integer[]> person = file.map(new Function<String, Integer[]>() {
            @Override
            public Integer[] call(String s) throws Exception {
                String[] temp = s.split("\t");
                Integer[] item = new Integer[temp.length];
                for(int i=0; i<item.length; i++){
                    item[i] = Integer.parseInt(temp[i]);
                }
                return item;
            }
        }).cache();

        long size = file.count();

        List<Partition> partitions = file.splits();

        System.out.println("Partition, size = " + partitions.size());

        System.out.println("Load All Datas, size = " + size);

        System.out.println("build person.flatMap");

        final Broadcast<List<Integer[]>> brc = sc.broadcast(person.collect());

        JavaRDD<String> result = person.flatMap(new FlatMapFunction<Integer[], String>() {
            @Override
            public Iterable<String> call(final Integer[] item) throws Exception {
                Date st1 = new Date();
                System.out.println("start matching " + item[0]);
                final List<Integer[]> cands = brc.value();
                final int size = cands.size();
                int batchSize = size / 10;
                BlockingQueue queue = new LinkedBlockingQueue();
                ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 1, TimeUnit.MINUTES, queue);
                final List<String> results = new ArrayList<String>();
                for(int i=item[0]; i<size;){
                    final int start = i;
                    final int end = start + batchSize;
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            for(int j=start;j<end && j<size;j++){
                                Integer[] row = cands.get(j);
                                double sum = 0.0;
                                for(int i=1; i<row.length;i++){
                                    sum += Math.abs(item[i] - row[i]);
                                }
                                System.out.println("mapping " + item[0] + ", " + row[0] + " ==> " + sum);
                                results.add(String.format("%s\t%s\t%s\n", item[0], row[0], sum));
                            }
                        }
                    });
                    i = end;
                }
                // Wait for threads to finish off.
                executor.shutdown();
                // Wait for everything to finish.
                while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Awaiting completion of threads.");
                }

                long ts = new Date().getTime() - st1.getTime();
                System.out.println("start matching DONE. ts=" + ts);

                return results;
            }
        });

        System.out.println("execute flatMap");

        long total = result.count();

        System.out.println("saveAsTextFile");

        //result.saveAsTextFile(hdfsUrl+"_result");

        System.out.println("total = " + total);
    }
}
