package org.example.ramenbrand;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

public class RamenBrandVarietyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text brandText = new Text();
    private Text varietyText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();

        String[] split = line.split(";");
        String brand = split[1];
        String variety = split[2];

        this.brandText.set(brand);
        this.varietyText.set(variety);
        output.collect(this.brandText, this.varietyText);
    }
}
