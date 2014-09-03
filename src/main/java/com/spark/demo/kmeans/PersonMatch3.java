package com.spark.demo.kmeans;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yaming_deng on 14-3-26.
 */
public class PersonMatch3 implements Serializable {

    public void start(String[] args){
        System.setProperty("spark.scheduler.mode", "FAIR");

        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]).setAppName("PersonMatch:3-P");
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

        //System.out.println("Load All Datas, size = " + size);

        JavaRDD<Integer[]> pdd = person.repartition(pfactor);

        class PartitionMark implements Serializable{
            public int id;
            public int left;
            public int right;

            @Override
            public int hashCode() {
                return id;
            }
        }

        int[] partionids = new int[pfactor];
        List<PartitionMark> tuple2s = new ArrayList<PartitionMark>(pfactor);
        int total = 0;
        for(int i=0; i<pfactor; i++){
            for(int j=0; j<pfactor; j++){
                PartitionMark pm = new PartitionMark();
                pm.id = total;
                pm.left = i;
                pm.right = j;
                tuple2s.add(pm);
                total ++;
            }
            partionids[i] = i;
        }

        System.out.println("build partion X partion = " + tuple2s.size());

        final Broadcast<List<Integer[]>[]> brc = sc.broadcast(pdd.collectPartitions(partionids));
        final Accumulator<Long> acc = sc.accumulator(0L, new AccumulatorParam<Long>() {
            @Override
            public Long addAccumulator(Long aLong, Long aLong2) {
                return aLong + aLong2;
            }

            @Override
            public Long addInPlace(Long aLong, Long aLong2) {
                return aLong + aLong2;
            }

            @Override
            public Long zero(Long aLong) {
                return aLong;
            }
        });
        final Accumulator<Integer> pcount = sc.accumulator(0);

        JavaRDD<PartitionMark> rdd =  sc.parallelize(tuple2s).repartition(tuple2s.size());

        System.out.println("build partion?? = " + rdd.splits().size());

        rdd.mapPartitions(new FlatMapFunction<Iterator<PartitionMark>, String>() {
            @Override
            public Iterable<String> call(Iterator<PartitionMark> itor) throws Exception {
                if (brc == null){
                    System.out.println("brc is NULL.");
                }
                final List<Integer[]>[] sets = brc.value();
                pcount.add(1);
                String[] results = null;
                int i = 0;
                if (itor.hasNext()){
                    PartitionMark pids = itor.next();
                    final List<Integer[]> list1 = sets[pids.left];
                    final List<Integer[]> list2 = sets[pids.right];
                    results = new String[list1.size() * list2.size()];
                    for (final Integer[] item1 : list1) {
                        for(final Integer[] item2 : list2){
                            double sum = 0;
                            for (int k = 1; k < item1.length; k++) {
                                int dt = item1[k] - item2[k];
                                sum += dt * dt;
                            }
                            sum = Math.sqrt(sum);
                            String str = String.format("%d\t%d\t%s", item1[0], item2[0], sum);
                            acc.add(1L);
                            results[i] = str;
                            i++;
                        }
                    }
                }else{
                    results = new String[0];
                }
                return Arrays.asList(results);
            }
        }).saveAsTextFile(hdfsUrl + "_result");

        System.out.println("total = " + acc.value());
        System.out.println("partition total = " + pcount.value());
        System.out.println("build partion X partion = " + tuple2s.size());
    }
}
