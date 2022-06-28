package org.example.ramencountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable revenueWritable = new IntWritable(0);
    private Text kotaText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();

        System.out.println("This is result of line variable");
        System.out.println(line);

        // Split based on '-'
        String[] split = line.split("-");
        String kota = split[0];
        kota = kota.trim(); // Delete space

        // Search for revenue
        String[] pendapatanSplit = split[1].split(" ");
        // Revenue in the end of index
        String pendapatan = pendapatanSplit[(pendapatanSplit.length-1)];

        // Report to collector
        this.kotaText.set(kota);
        this.revenueWritable.set(Integer.parseInt(pendapatan));
        output.collect(this.kotaText, this.revenueWritable);
    }
}
