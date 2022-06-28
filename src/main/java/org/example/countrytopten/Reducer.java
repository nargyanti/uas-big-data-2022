package org.example.countrytopten;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        System.out.println("-------------------------");
        System.out.println("This text is from reducer");
        System.out.println("Key: " + key.toString());
        System.out.println("-------------------------");

        int sum = 0;
        int kota = 0;
        int average = 0;

        while(values.hasNext()) {
            IntWritable currentRevenue = values.next();
            System.out.println("Current value: " + currentRevenue.get());
            sum += currentRevenue.get();
            ++kota;
            average = sum / kota;
        }
        output.collect(key, new IntWritable(average));
    }
}
