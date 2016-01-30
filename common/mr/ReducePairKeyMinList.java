package common.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;
import java.lang.Math;

import common.TextArrayWritable;
import common.Pair;

public class ReducePairKeyMinList extends Reducer<Text, TextArrayWritable, Text, Text> {
    Integer N;
    private Map<String, TreeSet<Pair<Float, String>>> mainKeyToTreeMap = new HashMap<String, TreeSet<Pair<Float, String>>>();

    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        this.N = conf.getInt("N", 10);
    }

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        if (! mainKeyToTreeMap.containsKey(key.toString())) {
            mainKeyToTreeMap.put(key.toString(), new TreeSet<Pair<Float, String>>());
        }
        TreeSet<Pair<Float, String>> tree = mainKeyToTreeMap.get(key.toString());

        for (TextArrayWritable val: values) {
            Text[] subList = (Text[]) val.toArray();
            for (Text subItem : subList) {
                String[] pair = subItem.toString().split("=");
                float x = Float.valueOf(pair[1]);
                Pair<Float, String> p = new Pair<Float, String>(x, pair[0]);
                tree.add(p);

                if (tree.size() > this.N) {
                    tree.remove(tree.last());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String mainKey : mainKeyToTreeMap.keySet()) {
            StringBuilder buf = new StringBuilder();
            for (Pair<Float, String> item : (TreeSet<Pair<Float, String>>) mainKeyToTreeMap.get(mainKey)) {
                if (buf.length() > 0) {
                    buf.append(" ");
                }
                buf.append(item.second + "=" + item.first.toString());
            }
            context.write(new Text(mainKey), new Text(buf.toString()));
        }
    }
}

