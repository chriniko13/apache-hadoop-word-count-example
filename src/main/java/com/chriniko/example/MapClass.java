package com.chriniko.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class MapClass extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //System.out.println("MapClass#map --- Key == " + key);

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), " ");

        while (stringTokenizer.hasMoreTokens()) {

            String word = stringTokenizer.nextToken();

            word = applyWordProcessors(
                    Arrays.asList(
                            String::trim,
                            w -> w.replace(".", "")
                    ),
                    word
            );

            context.write(
                    new Text(word),
                    new IntWritable(1)
            );
        }

    }

    @FunctionalInterface
    interface WordProcessor {
        String apply(String input);
    }

    private String applyWordProcessors(List<WordProcessor> wordProcessors, String input) {

        for (WordProcessor wordProcessor : wordProcessors) {
            input = wordProcessor.apply(input);
        }

        return input;
    }
}
