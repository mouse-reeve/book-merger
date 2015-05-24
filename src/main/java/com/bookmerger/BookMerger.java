package com.bookmerger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BookMerger {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "book merger");

        job.setJarByClass(BookMerger.class);
        job.setCombinerClass(BookDataReducer.class);
        job.setReducerClass(BookDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BookMapWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CanonicalMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, LibraryThingMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
