package org.example.brandtopten;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class BrandTopTenReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
    private Text brandName = new Text();

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        int count = 0;

        brandName = new Text(key.toString());
        for (Iterator<Text> it = values; it.hasNext(); ) {
            Text val = it.next();
            String strVal = val.toString();
            if(strVal.length() >= 1 && !strVal.equals("-")) {
                count += 1;
            } else if (strVal.length() < 1 && !strVal.equals("-")) {
                brandName = new Text(strVal);
            }
        }
        output.collect(brandName, new IntWritable(count));
    }
}
