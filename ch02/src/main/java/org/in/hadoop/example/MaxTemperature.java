package org.in.hadoop.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Date: 5/31/13
 * Time: 2:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperature {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }*/


        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max Temperature");

        File outputFile = new File("/media/traw/12F3E65C5D8A3FDF/project/java/hadoop/hadoop-example/output");
        deleteFile(outputFile);
        FileInputFormat.addInputPath(job, new Path("/media/traw/12F3E65C5D8A3FDF/project/java/hadoop/hadoop-example/input/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/media/traw/12F3E65C5D8A3FDF/project/java/hadoop/hadoop-example/output"));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void deleteFile(File file) {
        if (!file.exists())
            return;
        if (file.isDirectory()) {
            for (File file1 : file.listFiles()) {
                file1.delete();
            }
        }
        file.delete();
    }

}

