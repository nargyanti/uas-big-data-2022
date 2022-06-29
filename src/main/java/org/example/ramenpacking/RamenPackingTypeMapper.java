package org.example.ramenpacking;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class RamenPackingTypeMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable amountWritable = new IntWritable(0);
    private Text typeText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();

        String[] split = line.split(";");
        String type = split[3];
        int amount = 1;

        this.typeText.set(type);
        this.amountWritable.set(amount);
        output.collect(this.typeText, this.amountWritable);
    }
}
