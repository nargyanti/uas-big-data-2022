package org.example.ramenpacking;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class App 
{
    public static void main( String[] args )
    {
        JobConf conf = new JobConf(App.class);
        conf.setJobName("Type of Ramen Packing");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(RamenPackingTypeMapper.class);
        conf.setCombinerClass(RamenPackingTypeReducer.class);
        conf.setReducerClass(RamenPackingTypeReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
