package com.chriniko.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<Text, IntWritable> {


    @Override
    public int getPartition(Text text, IntWritable intWritable, int numReduceTasks) {

        System.out.println("PartitionerClass#PartitionerClass --- numReduceTasks == " + numReduceTasks);

        return ((text.toString().length() & Integer.MAX_VALUE) % numReduceTasks);
    }
}
