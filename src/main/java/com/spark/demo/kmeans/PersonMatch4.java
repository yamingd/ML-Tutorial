package com.spark.demo.kmeans;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by yaming_deng on 14-3-26.
 */
public class PersonMatch4 implements Serializable {

    static final Logger logger = LogManager.getLogger(PersonMatch4.class.getName());

    public void start(String[] args){
        System.setProperty("spark.scheduler.mode", "FAIR");

        SparkConf conf = new SparkConf();
        conf.setMaster(args[0]).setAppName("PersonMatch:4-PRS");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.cores.max", args[2]);
        conf.set("spark.executor.memory", args[3]);
        conf.set("spark.scheduler.allocation.file", "/usr/local/spark/spark/conf/fairscheduler.xml");
        conf.setSparkHome(System.getenv("SPARK_HOME"));
        conf.setJars(new String[]{System.getenv("SPARK_APP_JAR")});

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLocalProperty("spark.scheduler.pool", "production");

        this.start(sc, args);
    }

    public void start(JavaSparkContext sc, String[] args) {

        //String hdfsUrl = "hdfs://10.10.10.16:9000/tmp/eharmony_spark/user_data.txt";
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

        class KeySorter implements Serializable, Comparator<Integer[]>{

            @Override
            public int compare(Integer[] o1, Integer[] o2) {
                System.out.println("KeySorter");
                if (o1[0] > o2[0]){
                    return 1;
                }else if (o1[0] < o2[0]){
                    return -1;
                }else{
                    if (o1[1] > o2[1]){
                        return 1;
                    }else if (o1[1] < o2[1]){
                        return -1;
                    }
                }
                System.out.println("KeySorter");
                return 0;
            }
        }

        class TopSorter implements Serializable, Comparator<Tuple2<Integer[], Integer>>{

            @Override
            public int compare(Tuple2<Integer[], Integer> t1, Tuple2<Integer[], Integer> t2) {
                Integer[] o1 = t1._1;
                Integer[] o2 = t2._1;
                if (o1[0] > o2[0]){
                    return 1;
                }else if (o1[0] < o2[0]){
                    return -1;
                }else{
                    if (o1[1] > o2[1]){
                        return 1;
                    }else if (o1[1] < o2[1]){
                        return -1;
                    }
                }
                return 0;
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

        JavaRDD<PartitionMark> rdd =  sc.parallelize(tuple2s).repartition(tuple2s.size());

        System.out.println("build partion?? = " + rdd.splits().size());

        JavaPairRDD<Integer[], Integer> tdd1 = rdd.mapPartitions(new PairFlatMapFunction<Iterator<PartitionMark>, Integer[], Integer>() {
            @Override
            public Iterable<Tuple2<Integer[], Integer>> call(Iterator<PartitionMark> itor) throws Exception {
                pcount.add(1);
                final List<Integer[]>[] sets = brc.value();
                if (itor.hasNext()) {
                    int i = 0;
                    Tuple2<Integer[], Integer>[] results = null;
                    PartitionMark pids = itor.next();
                    final List<Integer[]> list1 = sets[pids.left];
                    final List<Integer[]> list2 = sets[pids.right];
                    results = new Tuple2[list1.size() * list2.size()];
                    for (final Integer[] item1 : list1) {
                        for (final Integer[] item2 : list2) {
                            double sum = 0;
                            for (int k = 1; k < item1.length; k++) {
                                int dt = item1[k] - item2[k];
                                sum += dt * dt;
                            }
                            sum = Math.sqrt(sum);
                            int isum = (int) (10000 * sum);
                            Tuple2 tt = new Tuple2<Integer[], Integer>(new Integer[]{item1[0], isum}, item2[0]);
                            results[i] = tt;
                            i++;
                            acc.add(1L);
                        }
                    }
                    return Arrays.asList(results);
                }else{
                    return new ArrayList<Tuple2<Integer[], Integer>>();
                }
            }
        }).persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        JavaPairRDD<Integer[], Integer> tdd2 = tdd1.sortByKey(new KeySorter(), false).persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        tdd2.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer[], Integer>>, String>() {
            @Override
            public Iterable<String> call(Iterator<Tuple2<Integer[], Integer>> itor) throws Exception {
                List<String> results = new ArrayList<String>();
                while(itor.hasNext()){
                    Tuple2<Integer[], Integer> tuple2 = itor.next();
                    String str = String.format("%d\t%d\t%s", tuple2._1[0], tuple2._2, ((float) tuple2._1[1]) / 10000);
                    results.add(str);
                }
                return results;
            }
        }).saveAsTextFile(hdfsUrl + "_result");

        System.out.println("total = " + acc.value());
        System.out.println("partition total = " + pcount.value());
        System.out.println("build partion X partion = " + tuple2s.size());
    }
}
