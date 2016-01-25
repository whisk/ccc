package common.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import common.Pair;
import common.TextArrayWritable;

// maps : (key1-key2 val12) (key1-key3 val13) ... -> (key1 key2=val12|key3=val13)
public class MapPairKeyMinList extends Mapper<Object, Text, Text, TextArrayWritable> {
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
