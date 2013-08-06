import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Project: Hadoop_Yina
 * User: szhao
 * Date: 8/6/13
 * Time: 12:44 PM
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split(" ");
        for (String val : vals) {
            writeKey.set(val);
            context.write(writeKey, ONE);
        }
    }

    private LongWritable ONE = new LongWritable(1);
    private Text writeKey = new Text();
}
