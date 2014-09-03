package com.tutorial.bigdata.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-10.
 */
public class PersonAttrProfileMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    public PersonAttrProfileMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] orginStr = value.toString().split("\t");
        for(int i=1; i<orginStr.length; i++){
            String skey = i + "\t" + orginStr[i];
            context.write(new Text(skey), new IntWritable(1));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
