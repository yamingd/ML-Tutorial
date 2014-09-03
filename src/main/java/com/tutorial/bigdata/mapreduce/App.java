package com.tutorial.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yaming_deng on 14-4-10.
 */
public class App extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        String inputFilePath = "hdfs://10.10.9.156:9000/tmp/eharmony_spark/user_data_5w.txt";

        // 设置job
        Job job = Job.getInstance();
        JobConf conf = (JobConf)job.getConfiguration();
        conf.set(NLineInputFormat.LINES_PER_MAP, "500");

        job.setJobName("PersonAttrProfileMapper");

        // 设置jar包入口
        job.setJarByClass(App.class);

        // 设置mapper类和参数类型
        job.setMapperClass(PersonAttrProfileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setCombinerClass(PersonAttrProfileCombiner.class);
        job.setReducerClass(PersonAttrProfileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入
        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        // 输出
        FileOutputFormat.setOutputPath(job, new Path(inputFilePath+"_mroutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
