package com.spark.demo.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.Vector;

import java.io.File;
import java.util.List;

/**
 * Created by yamingd on 14-1-23.
 */
public class AppKMeans {


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: JavaKMeans <master> <file> <k> <convergeDist>");
            //System.exit(1);
        }

        System.setProperty("spark.scheduler.mode", "FAIR");

        JavaSparkContext sc = new JavaSparkContext(args[0], "JavaKMeans2",
                System.getenv("SPARK_HOME"), System.getenv("SPARK_APP_JAR"));

        System.out.println("SPARK_HOME: " + System.getenv("SPARK_HOME"));
        System.out.println("SPARK_APP_JAR: " + System.getenv("SPARK_APP_JAR"));

        String path = args[1]; //数据存放路径
        int K = Integer.parseInt(args[2]); //分类个数
        double convergeDist = Double.parseDouble(args[3]); //衡量分类好坏的临界值

        KMeans kMeans = new KMeans();
        JavaRDD<Vector> data = kMeans.loadTrainData(sc, path);
        final List<Vector> centerids = kMeans.trainCenter(sc, data, K, convergeDist);

        File folder = new File(path).getParentFile();
        String filePath = folder.getAbsolutePath() + File.separator + "centerids";
        sc.parallelize(centerids).saveAsObjectFile(filePath);

        System.exit(0);

    }

}
