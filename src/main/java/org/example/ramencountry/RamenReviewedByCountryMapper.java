package org.example.ramencountry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class RamenReviewedByCountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text countryText = new Text();
    private Text reviewText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();

        String[] split = line.split(";");
        String country = split[4];
        String brand = split[0];

        this.countryText.set(country);
        this.reviewText.set(brand);
        output.collect(this.countryText, this.reviewText);
    }
}
