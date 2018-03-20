package com.chriniko.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //System.out.println("ReduceClass#reduce --- Key == " + key);

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        Counter counter = context.getCounter("ReduceClass-Group", "reduceOperations");
        counter.increment(1);

        context.write(key, new IntWritable(sum));
    }
}
