package common.mr;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ReduceAverage extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        long cnt = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            cnt += 1;
        }
        context.write(key, new FloatWritable(sum / cnt));
    }
}

