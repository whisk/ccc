package task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;

import common.ImprovedTask;
import common.TextArrayWritable;
import common.Pair;
import common.mr.ReduceAverage;
import common.mr.MapPairKeyMinList;
import common.mr.ReducePairKeyMinList;

public class Task22 extends ImprovedTask implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task22(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // init
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path pathInputPrefix = new Path(args[0]);
        Path pathWorkPrefix  = new Path(args[1]);
        Path tmpPath = Path.mergePaths(pathWorkPrefix, new Path("/tmp"));
        fs.delete(tmpPath, true);
        Path outPath = Path.mergePaths(pathWorkPrefix, new Path("/output"));
        fs.delete(outPath, true);

        Job jobA = Job.getInstance(conf, "Origin-Destination Departure Delay");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(FloatWritable.class);

        jobA.setMapperClass(OriginDestinationDepDelayMap.class);
        jobA.setReducerClass(ReduceAverage.class);

        FileInputFormat.setInputPaths(jobA, pathInputPrefix);
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(Task22.class);

        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Origin Top Departure Performance by Destination");

        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapperClass(MapPairKeyMinList.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);
        jobB.setReducerClass(ReducePairKeyMinList.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, outPath);

        jobB.setJarByClass(Task22.class);

        return jobB.waitForCompletion(true)? 0 : 1;
    }

    public static class OriginDestinationDepDelayMap extends Mapper<Object, Text, Text, FloatWritable> {
        @Override
        public void map(Object lineNum, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            try {
                String origin = row[5];
                String destination = row[6];
                Float depDelay = Float.parseFloat(row[8]);
                
                String orgDest = (origin + "-" + destination).toUpperCase();
                context.write(new Text(orgDest), new FloatWritable(depDelay));
            } catch (Exception e) {
                // skip on error parsing
            }
        }
    }
}
