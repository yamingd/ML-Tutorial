package com.spark.demo.kmeans;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.Vector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by yamingd on 14-1-23.
 */
public class KMeans implements Serializable {

    public JavaRDD<Vector> loadTrainData(JavaSparkContext sc,String path){
        //读取数据
        JavaRDD<Vector> data = sc.textFile(path).map(
                new Function<String, Vector>() {
                    @Override
                    public Vector call(String line) {
                        return VectorUtils.parseVector(line);
                    }
                }
        ).cache();

        return data;
    }

    public List<Vector> trainCenter(JavaSparkContext sc, JavaRDD<Vector> data, int K, double convergeDist){
        //为每个类别(Cluster)随机选取一个中心点
        final List<Vector> centroids = data.takeSample(false, K, 42);

        double tempDist;
        int round = 1000;
        do {
            // 将数据集向量按到中心点的距离分到各个类别内(Cluster)
            System.out.println("Step1: " + new Date());
            JavaPairRDD<Integer, Vector> closest = data.map(
                    new PairFunction<Vector, Integer, Vector>() {
                        @Override
                        public Tuple2<Integer, Vector> call(Vector vector) {
                            return new Tuple2<Integer, Vector>(
                                    VectorUtils.closestPoint(vector, centroids), vector);
                        }
                    }
            );

            // 计算每个类别(Cluster)向量的平均值
            System.out.println("Step2: " + new Date());
            JavaPairRDD<Integer, List<Vector>> pointsGroup = closest.groupByKey();
            Map<Integer, Vector> newCentroids = pointsGroup.mapValues(
                    new Function<List<Vector>, Vector>() {
                        @Override
                        public Vector call(List<Vector> ps) {
                            return VectorUtils.average(ps);
                        }
                    }).collectAsMap();


            System.out.println("Step3: " + new Date());
            tempDist = 0.0;
            for (int i = 0; i < K; i++) {
                tempDist += centroids.get(i).squaredDist(newCentroids.get(i));
            }
            // 重新定位每个类别的中心点(向量)
            System.out.println("Step4: " + new Date());
            for (Map.Entry<Integer, Vector> t: newCentroids.entrySet()) {
                centroids.set(t.getKey(), t.getValue());
            }

            round --;
            System.out.println("Finished iteration (delta = " + tempDist + ")");

        } while (tempDist > convergeDist && round > 0);

        System.out.println("Step5: " + new Date());
        System.out.println("Final centers:");
        for (Vector c : centroids) {
            System.out.println(c);
        }

        return centroids;
    }
}
