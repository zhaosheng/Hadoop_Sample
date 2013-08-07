/**
 * Created with IntelliJ IDEA.
 * Project: Hadoop_Sample
 * User: szhao
 * Date: 8/6/13
 * Time: 12:45 PM
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // Reduce function goes here
        long sum = 0;

        for (LongWritable value : values) {
            sum += value.get();
        }
        writeVal.set(sum);
        context.write(key, writeVal);
    }

    private LongWritable writeVal = new LongWritable();
}