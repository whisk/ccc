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
import org.apache.hadoop.io.DoubleWritable;
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

public class Task24 extends ImprovedTask implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task24(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // init
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path pathInputPrefix = new Path(args[0]);
        Path pathWorkPrefix  = new Path(args[1]);
        Path outPath = Path.mergePaths(pathWorkPrefix, new Path("/output"));
        fs.delete(outPath, true);

        Job jobA = Job.getInstance(conf, "Origin-Destination mean arrival delay");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(DoubleWritable.class);

        jobA.setMapperClass(OriginDestinationArrivalDelayMap.class);
        jobA.setReducerClass(ReduceAverage.class);

        FileInputFormat.setInputPaths(jobA, Path.mergePaths(pathInputPrefix, new Path("/input")));
        FileOutputFormat.setOutputPath(jobA, outPath);

        jobA.setJarByClass(Task24.class);

        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class OriginDestinationArrivalDelayMap extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object lineNum, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            try {
                String origin = row[1];
                String destination = row[2];
                double arrDelay = Double.parseDouble(row[4]);
                
                String key = (origin + "_" + destination).toUpperCase();
                context.write(new Text(key), new DoubleWritable(arrDelay));
            } catch (Exception e) {
                // skip on error parsing
            }
        }
    }
}
