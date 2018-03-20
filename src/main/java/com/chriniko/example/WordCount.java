package com.chriniko.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    /*
        Note: Program arguments: 1 ---> input.txt 2 ---> output
     */
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);

    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        // Create a new Jar and set the driver class(this class) as the main class of jar.
        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCounter");

        job.setNumReduceTasks(4);

        //Set the input and the output path from the arguments.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the map and reduce classes in the job.
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        //Run the job and wait for its completion.
        boolean isSuccessful = job.waitForCompletion(true);

        if (isSuccessful) {
            System.out.println("Job was successful");
        } else {
            System.out.println("Job was not successful");
        }


        //Print counters before finish
        for (CounterGroup counterGroup : job.getCounters()) {

            for (Counter counter : counterGroup) {

                System.out.println("Counter (groupName = "
                        + counterGroup.getName()
                        + ", name = "
                        + counter.getName()
                        + ") --- value = "
                        + counter.getValue());

            }
        }

        return isSuccessful ? 1 : 0;

    }
}
