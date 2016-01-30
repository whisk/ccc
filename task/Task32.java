package task;

import org.apache.log4j.Logger;
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
import java.util.Formatter;

import common.ImprovedTask;
import common.TextArrayWritable;
import common.Pair;
import common.mr.ReduceAverage;
import common.mr.MapPairKeyMinList;
import common.mr.ReducePairKeyMinList;

public class Task32 extends ImprovedTask implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task32(), args);
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

        Job jobA = Job.getInstance(conf, "Origin-Destination-Date-DepTime Mean Arrival Delay");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(FloatWritable.class);

        jobA.setMapperClass(OriginDestinationDateDepTimeArrDelayMap.class);
        jobA.setReducerClass(ReduceAverage.class);

        FileInputFormat.setInputPaths(jobA, pathInputPrefix);
        FileOutputFormat.setOutputPath(jobA, outPath);

        jobA.setJarByClass(Task32.class);

        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class OriginDestinationDateDepTimeArrDelayMap extends Mapper<Object, Text, Text, FloatWritable> {
        private Logger log = Logger.getLogger(this.getClass());

        @Override
        public void map(Object lineNum, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\s");
            try {
                String origin = row[5];
                String destination = row[6];
                Formatter formatter = new Formatter(new StringBuilder());
                String date = formatter.format("%04d:%02d:%02d", Integer.parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2])).toString();
                String time = row[7];
                Float arrDelay = Float.parseFloat(row[9]);
                
                String key = (origin + "_" + destination + "_" + date + "_" + time).toUpperCase();
                context.write(new Text(key), new FloatWritable(arrDelay));
            } catch (Exception e) {
                // skip on error parsing
                log.error(e);
            }
        }
    }
}
