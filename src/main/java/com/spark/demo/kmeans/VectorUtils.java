package com.spark.demo.kmeans;

import org.apache.spark.util.Vector;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by yamingd on 14-1-23.
 */
public class VectorUtils {

    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     * 转换文本行为向量Vector
     * @param line
     * @return
     */
    public static Vector parseVector(String line) {
        String[] splits = SPACE.split(line);
        double[] data = new double[splits.length];
        int i = 0;
        for (String s : splits) {
            data[i] = Double.parseDouble(s);
            i++;
        }
        return new Vector(data);
    }

    /**
     * 计算每个向量最近点
     * @param p
     * @param centers
     * @return
     */
    public static int closestPoint(Vector p, List<Vector> centers) {
        int bestIndex = 0;
        double closest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centers.size(); i++) {
            double tempDist = p.squaredDist(centers.get(i));
            if (tempDist < closest) {
                closest = tempDist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    /**
     * 计算每个类别(cluster)的平均值
     * 类别即由一些列的向量组成
     * @param ps
     * @return
     */
    public static Vector average(List<Vector> ps) {
        int numVectors = ps.size();
        Vector out = new Vector(ps.get(0).elements());
        // start from i = 1 since we already copied index 0 above
        for (int i = 1; i < numVectors; i++) {
            out.addInPlace(ps.get(i));
        }
        return out.divide(numVectors);
    }
}
