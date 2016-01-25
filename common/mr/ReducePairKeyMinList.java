package common.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;

import common.TextArrayWritable;
import common.Pair;

public class ReducePairKeyMinList extends Reducer<Text, TextArrayWritable, Text, Text> {
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
                buf.append(" ");
            }
            buf.append(item.second + "=" + item.first.toString());
        }
        context.write(key, new Text(buf.toString()));
    }
}

