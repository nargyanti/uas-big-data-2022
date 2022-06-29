package org.example.ramencountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class RamenReviewedByCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
    private Text countryName = new Text();
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        int count = 0;
        countryName = new Text(key.toString());
        for (Iterator<Text> it = values; it.hasNext(); ) {
            Text val = it.next();
            String strVal = val.toString();
            if(strVal.length() >= 1) {
                count += 1;
            } else {
                countryName = new Text(strVal);
            }
        }
        output.collect(countryName, new IntWritable(count));
    }
}
