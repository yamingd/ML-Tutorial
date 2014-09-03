package com.tutorial.bigdata.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        String hdfsUrl = "hdfs://10.10.9.156:9000/tmp/eharmony/data_result/part-m-00000";

        Job job = Job.getInstance();

        // 设置jar包入口
        job.setJarByClass(WordCount.class);

        job.setMapperClass(CountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(CountReduce.class);

        job.setReducerClass(CountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(hdfsUrl));
        FileOutputFormat.setOutputPath(job, new Path(hdfsUrl+"_wc"));

        job.waitForCompletion(true);
    }

}
