package com.spark.demo.kmeans;

/**
 * Created by yaming_deng on 14-3-26.
 */
public class AppMatch {

    public static void main(String[] args) throws Exception {

        System.out.println("SPARK_HOME: " + System.getenv("SPARK_HOME"));
        System.out.println("SPARK_APP_JAR: " + System.getenv("SPARK_APP_JAR"));


        if("5".equals(args[1])){
            PersonMatch5 personMatch = new PersonMatch5();
            personMatch.start(args);
        } else if("3".equals(args[1])){
            PersonMatch3 personMatch = new PersonMatch3();
            personMatch.start(args);
        }else if("4".equals(args[1])){
            PersonMatch4 personMatch = new PersonMatch4();
            personMatch.start(args);
        }else if("2".equals(args[1])){
            PersonMatch2 personMatch = new PersonMatch2();
            personMatch.start(args);
        }else if("6".equals(args[1])){
            PersonAttrProfile task = new PersonAttrProfile();
            task.start(args);
        }else if("7".equals(args[1])){
            PersonAttrPairProfile task = new PersonAttrPairProfile();
            task.start(args);
        }


        System.exit(0);

    }
}
