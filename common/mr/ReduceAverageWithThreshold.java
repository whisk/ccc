package common.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ReduceAverageWithThreshold extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    Integer M;

    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        this.M = conf.getInt("M", 100);
    }

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        long cnt = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            cnt += 1;
        }
        // output only if items count is over the threshold
        if (cnt >= this.M) {
            context.write(key, new FloatWritable(sum / cnt));
        }
    }
}

