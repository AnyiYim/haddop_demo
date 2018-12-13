//package com.examples.MaxTemperature;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.charAt(0) != 'S') {
            String year = line.substring(14, 18);
            int airTemperature;
            String tem = line.substring(24, 28).trim();
            airTemperature = Integer.parseInt(tem);
            context.write(new Text(year), new IntWritable(airTemperature));
//
//            if (airTemperature != 999) {
//                context.write(new Text(year), new IntWritable(airTemperature));
//            }
        }

    }

}
