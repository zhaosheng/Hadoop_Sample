import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * Project: Hadoop_Sample
 * User: szhao
 * Date: 8/6/13
 * Time: 10:58 AM
 */
public class WordCount extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();

        // This line is to hack the chmod exception in Windows, please remove before production release!
        conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath))              // purge the output folder if it already exists.
            fs.delete(outputPath, true);


        Job job = new Job(conf, "Word Counter");
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);             // Set the job input format as text
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);           // Set the job output format as text

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);                                   // Set the reducer number

        job.setMapperClass(WordCountMapper.class);                  // Set the Mapper class
        job.setReducerClass(WordCountReducer.class);                // Set the Reducer class
        job.setCombinerClass(WordCountReducer.class);               // Set the Combiner class




        return job.waitForCompletion(true) ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new WordCount(), args);
        System.exit(run);
    }



}


