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

// >>> Don't Change
public class Task11 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task11(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // init
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path pathPrefix = new Path(args[0]);
        Path orgCountPath = Path.mergePaths(pathPrefix, new Path("/tmp_org"));
        fs.delete(orgCountPath, true);
        Path dstCountPath = Path.mergePaths(pathPrefix, new Path("/tmp_dst"));
        fs.delete(dstCountPath, true);
        Path orgOutPath = Path.mergePaths(pathPrefix, new Path("/output_org"));
        fs.delete(orgOutPath, true);
        Path dstOutPath = Path.mergePaths(pathPrefix, new Path("/output_dst"));
        fs.delete(dstOutPath, true);

        // count origins
        Job jobOrg = Job.getInstance(conf, "Origins Count");
        jobOrg.setOutputKeyClass(Text.class);
        jobOrg.setOutputValueClass(IntWritable.class);

        jobOrg.setMapperClass(OriginCountMap.class);
        jobOrg.setReducerClass(CountReduce.class);

        FileInputFormat.setInputPaths(jobOrg, Path.mergePaths(pathPrefix, new Path("/input")));
        FileOutputFormat.setOutputPath(jobOrg, orgCountPath);

        jobOrg.setJarByClass(Task11.class);

        // count destinations
        Job jobDst = Job.getInstance(conf, "Destinations Count");
        jobDst.setOutputKeyClass(Text.class);
        jobDst.setOutputValueClass(IntWritable.class);

        jobDst.setMapperClass(DestinationCountMap.class);
        jobDst.setReducerClass(CountReduce.class);

        FileInputFormat.setInputPaths(jobDst, Path.mergePaths(pathPrefix, new Path("/input")));
        FileOutputFormat.setOutputPath(jobDst, dstCountPath);

        jobDst.setJarByClass(Task11.class);
        
        // run count
        jobOrg.waitForCompletion(true);
        jobDst.waitForCompletion(true);

        // top origins
        Job jobTopOrg = Job.getInstance(conf, "Top Origins");
        jobTopOrg.setOutputKeyClass(Text.class);
        jobTopOrg.setOutputValueClass(IntWritable.class);

        jobTopOrg.setMapOutputKeyClass(NullWritable.class);
        jobTopOrg.setMapOutputValueClass(TextArrayWritable.class);

        jobTopOrg.setMapperClass(TopMap.class);
        jobTopOrg.setReducerClass(TopReduce.class);
        jobTopOrg.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobTopOrg, orgCountPath);
        FileOutputFormat.setOutputPath(jobTopOrg, orgOutPath);

        jobTopOrg.setInputFormatClass(KeyValueTextInputFormat.class);
        jobTopOrg.setOutputFormatClass(TextOutputFormat.class);

        jobTopOrg.setJarByClass(Task11.class);

        // top destinations
        Job jobTopDst = Job.getInstance(conf, "Top Destinations");
        jobTopDst.setOutputKeyClass(Text.class);
        jobTopDst.setOutputValueClass(IntWritable.class);

        jobTopDst.setMapOutputKeyClass(NullWritable.class);
        jobTopDst.setMapOutputValueClass(TextArrayWritable.class);

        jobTopDst.setMapperClass(TopMap.class);
        jobTopDst.setReducerClass(TopReduce.class);
        jobTopDst.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobTopDst, dstCountPath);
        FileOutputFormat.setOutputPath(jobTopDst, dstOutPath);

        jobTopDst.setInputFormatClass(KeyValueTextInputFormat.class);
        jobTopDst.setOutputFormatClass(TextOutputFormat.class);

        jobTopDst.setJarByClass(Task11.class);

        // run top origins and destinations
        return jobTopOrg.waitForCompletion(true) && jobTopDst.waitForCompletion(true)? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class OriginCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            context.write(new Text(row[0].toUpperCase()), new IntWritable(1));
        }
    }

    public static class DestinationCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            context.write(new Text(row[1].toUpperCase()), new IntWritable(1));
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
            this.N = conf.getInt("N", 10);
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

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
