package common.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ReduceAverageWithThreshold extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    Integer M;

    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        this.M = conf.getInt("M", 100);
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        long cnt = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            cnt += 1;
        }
        // output only if items count is over the threshold
        if (cnt >= this.M) {
            context.write(key, new DoubleWritable(sum / cnt));
        }
    }
}

