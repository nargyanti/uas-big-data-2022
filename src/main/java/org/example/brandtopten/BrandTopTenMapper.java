package org.example.brandtopten;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class BrandTopTenMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
    private Text topTenText = new Text();
    private Text brandText = new Text();

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();

        String[] split = line.split(";");
        String brand = split[1];
        String topTen = split[6];

        this.brandText.set(brand);
        this.topTenText.set(topTen);
        output.collect(this.brandText, this.topTenText);
    }
}
