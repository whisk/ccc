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
import common.Pair;
import common.TextArrayWritable;

public class Task11 extends ImprovedTask implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task11(), args);
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

        // count origins and destinations
        Job jobA = Job.getInstance(conf, "Popularity Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(PopularityCountMap.class);
        jobA.setReducerClass(CountReduce.class);

        FileInputFormat.setInputPaths(jobA, pathInputPrefix);
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(Task11.class);

        jobA.waitForCompletion(true);

        // top popularity
        Job jobB = Job.getInstance(conf, "Top Popularity");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopMap.class);
        jobB.setReducerClass(TopReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, outPath);

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(Task11.class);

        // run top origins and destinations
        return jobB.waitForCompletion(true)? 0 : 1;
    }

    public static class PopularityCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            // origin
            context.write(new Text(row[5].toUpperCase()), new IntWritable(1));
            // destination
            context.write(new Text(row[6].toUpperCase()), new IntWritable(1));
        }
    }

    public static class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        private TreeSet<Pair<Integer, String>> countToWordMap = new TreeSet<Pair<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            String word = key.toString();

            countToWordMap.add(new Pair<Integer, String>(count, word));

            if (countToWordMap.size() > this.N) {
                countToWordMap.remove(countToWordMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, String> item : countToWordMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        private TreeSet<Pair<Integer, String>> countToWordMap = new TreeSet<Pair<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 100);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair= (Text[]) val.toArray();

                String word = pair[0].toString();
                Integer count = Integer.parseInt(pair[1].toString());

                countToWordMap.add(new Pair<Integer, String>(count, word));

                if (countToWordMap.size() > this.N) {
                    countToWordMap.remove(countToWordMap.first());
                }
            }

            for (Pair<Integer, String> item: countToWordMap) {
                Text word = new Text(item.second);
                IntWritable value = new IntWritable(item.first);
                context.write(word, value);
            }
        }
    }
}
