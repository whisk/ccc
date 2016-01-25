package common.mr;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ReduceAverage extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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

