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
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

import common.ImprovedTask;
import common.TextArrayWritable;
import common.Pair;
import common.mr.ReduceAverage;

public class Task12 extends ImprovedTask implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task12(), args);
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
        Path outCarrierPath = Path.mergePaths(pathWorkPrefix, new Path("/output_carrier"));
        fs.delete(outCarrierPath, true);
        Path outWeekdayPath = Path.mergePaths(pathWorkPrefix, new Path("/output_weekday"));
        fs.delete(outWeekdayPath, true);

        // Carrier Delay
        Job jobCD = Job.getInstance(conf, "Carrier Delay");
        jobCD.setOutputKeyClass(Text.class);
        jobCD.setOutputValueClass(DoubleWritable.class);

        jobCD.setMapperClass(CarrierArrDelayMap.class);
        jobCD.setReducerClass(ReduceAverage.class);

        FileInputFormat.setInputPaths(jobCD, pathInputPrefix);
        FileOutputFormat.setOutputPath(jobCD, tmpPath);

        jobCD.setJarByClass(Task12.class);

        jobCD.waitForCompletion(true);

        // Weekday Delay
        Job jobWD = Job.getInstance(conf, "Weekday Delay");
        jobWD.setOutputKeyClass(Text.class);
        jobWD.setOutputValueClass(DoubleWritable.class);

        jobWD.setMapperClass(WeekdayDelayMap.class);
        jobWD.setReducerClass(ReduceAverage.class);

        FileInputFormat.setInputPaths(jobWD, pathInputPrefix);
        FileOutputFormat.setOutputPath(jobWD, outWeekdayPath);

        jobWD.setJarByClass(Task12.class);

        jobWD.waitForCompletion(true);

        // min carrier delay
        Job jobMD = Job.getInstance(conf, "Min Carrier Delay");
        jobMD.setOutputKeyClass(Text.class);
        jobMD.setOutputValueClass(DoubleWritable.class);

        jobMD.setMapOutputKeyClass(NullWritable.class);
        jobMD.setMapOutputValueClass(TextArrayWritable.class);

        jobMD.setMapperClass(MinMap.class);
        jobMD.setReducerClass(MinReduce.class);
        jobMD.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobMD, tmpPath);
        FileOutputFormat.setOutputPath(jobMD, outCarrierPath);

        jobMD.setInputFormatClass(KeyValueTextInputFormat.class);
        jobMD.setOutputFormatClass(TextOutputFormat.class);

        jobMD.setJarByClass(Task12.class);

        // run top origins and destinations
        return jobMD.waitForCompletion(true)? 0 : 1;
    }

    public static class CarrierArrDelayMap extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            // sometimes delay not specified
            try {
                double arrDelay = Double.parseDouble(row[9]);
                context.write(new Text(row[4].toUpperCase()), new DoubleWritable(arrDelay));
            } catch (Exception e) {
            }
        }
    }

    public static class WeekdayDelayMap extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            // sometimes delay not specified
            double delay = 0.0;
            try {
                delay = Double.parseDouble(row[2]);
            } catch (Exception e) {
            }

            context.write(new Text(row[0]), new DoubleWritable(delay));
        }
    }

    public static class MinMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        private TreeSet<Pair<Double, String>> countToWordMap = new TreeSet<Pair<Double, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Double count = Double.parseDouble(value.toString());
            String word = key.toString();

            countToWordMap.add(new Pair<Double, String>(count, word));

            if (countToWordMap.size() > this.N) {
                countToWordMap.remove(countToWordMap.last());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Double, String> item : countToWordMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class MinReduce extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
        Integer N;
        private TreeSet<Pair<Double, String>> countToWordMap = new TreeSet<Pair<Double, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair= (Text[]) val.toArray();

                String word = pair[0].toString();
                Double count = Double.parseDouble(pair[1].toString());

                countToWordMap.add(new Pair<Double, String>(count, word));

                if (countToWordMap.size() > this.N) {
                    countToWordMap.remove(countToWordMap.last());
                }
            }

            for (Pair<Double, String> item: countToWordMap) {
                Text word = new Text(item.second);
                DoubleWritable value = new DoubleWritable(item.first);
                context.write(word, value);
            }
        }
    }

}
