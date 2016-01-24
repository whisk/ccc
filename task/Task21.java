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
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;

import common.TextArrayWritable;
import common.Pair;

public class Task21 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task21(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // init
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path pathPrefix = new Path(args[0]);
        Path tmpPath = Path.mergePaths(pathPrefix, new Path("/tmp"));
        fs.delete(tmpPath, true);
        Path outPath = Path.mergePaths(pathPrefix, new Path("/output"));
        fs.delete(outPath, true);

        // Origin-Carrier Departure Delay
        Job jobOCD = Job.getInstance(conf, "Origin-Carrier Departure Delay");
        jobOCD.setOutputKeyClass(Text.class);
        jobOCD.setOutputValueClass(DoubleWritable.class);

        jobOCD.setMapperClass(OriginCarrierDepDelayMap.class);
        jobOCD.setReducerClass(ReduceAverage.class);

        FileInputFormat.setInputPaths(jobOCD, Path.mergePaths(pathPrefix, new Path("/input")));
        FileOutputFormat.setOutputPath(jobOCD, tmpPath);

        jobOCD.setJarByClass(Task21.class);

        jobOCD.waitForCompletion(true);

        // Origin-Carrier top departure performance 
        Job jobOCP = Job.getInstance(conf, "Origin-Carrier top departure performance");

        jobOCP.setOutputKeyClass(Text.class);
        jobOCP.setOutputValueClass(Text.class);

        jobOCP.setMapperClass(MapSplitKeyMinList.class);
        jobOCP.setMapOutputValueClass(TextArrayWritable.class);
        jobOCP.setReducerClass(ReduceSplitKeyMinList.class);
        jobOCP.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobOCP, tmpPath);
        FileOutputFormat.setOutputPath(jobOCP, outPath);

        jobOCP.setJarByClass(Task21.class);

        return jobOCP.waitForCompletion(true)? 0 : 1;
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


    public static class OriginCarrierDepDelayMap extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object lineNum, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            try {
                String origin = row[1];
                String carrier = row[0];
                double depDelay = Double.parseDouble(row[3]);
                
                String orgCarr = (origin + "-" + carrier).toUpperCase();
                context.write(new Text(orgCarr), new DoubleWritable(depDelay));
            } catch (Exception e) {
                // skip on error parsing
            }
        }
    }

    // TODO: extract
    public static class ReduceAverage extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            long cnt = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                cnt += 1;
            }
            context.write(key, new DoubleWritable(sum / cnt));
        }
    }

    // TODO: extract
    // maps : (key1-key2 val12) (key1-key3 val13) ... -> (key1 key2=val12|key3=val13)
    public static class MapSplitKeyMinList extends Mapper<Object, Text, Text, TextArrayWritable> {
        Integer N;
        private Map<String, TreeSet<Pair<Double,String>>> mainKeyToTreeMap = new HashMap<String, TreeSet<Pair<Double,String>>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Object lineNum, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            String key = row[0];
            Double val = Double.parseDouble(row[1].toString());
            // subkey is "key0-key1" pair
            String[] subkey = key.toString().split("-"); // key[0] is the main key

            if (! mainKeyToTreeMap.containsKey(subkey[0])) {
                mainKeyToTreeMap.put(subkey[0], new TreeSet<Pair<Double, String>>());
            }
            TreeSet<Pair<Double, String>> tree = mainKeyToTreeMap.get(subkey[0]);
            tree.add(new Pair<Double, String>(val, subkey[1])); // add secondary key

            if (tree.size() > this.N) {
                tree.remove(tree.last());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String mainKey : mainKeyToTreeMap.keySet()) {
                List<String> values = new ArrayList<String>();
                for (Pair<Double, String> item : (TreeSet<Pair<Double, String>>) mainKeyToTreeMap.get(mainKey)) {
                    values.add(item.second + "=" + item.first.toString());
                }
                context.write(new Text(mainKey), new TextArrayWritable((String[]) values.toArray(new String[0])));
            }
        }
    }

    // TODO: extract
    public static class ReduceSplitKeyMinList extends Reducer<Text, TextArrayWritable, Text, Text> {
        Integer N;
        private TreeSet<Pair<Double, String>> valToKeyMap = new TreeSet<Pair<Double, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] subList = (Text[]) val.toArray();
                for (Text subItem : subList) {
                    String[] pair = subItem.toString().split("=");
                    try {
                        String subkey = pair[0];
                        Double subval = Double.parseDouble(pair[1].toString());

                        valToKeyMap.add(new Pair<Double, String>(subval, subkey));

                        if (valToKeyMap.size() > this.N) {
                            valToKeyMap.remove(valToKeyMap.last());
                        }
                    } catch (Exception e) { 
                    }
                }
            }

            StringBuilder buf = new StringBuilder();
            for (Pair<Double, String> item : valToKeyMap) {
                if (buf.length() > 0) {
                    buf.append("|");
                }
                buf.append(item.second + "=" + item.first.toString());
            }
            context.write(key, new Text(buf.toString()));
        }
    }

}
