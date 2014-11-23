package candles;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Candles {
    public static Configuration conf;
    public static FileSystem fs;
    public static DateFormat dateFormat = new SimpleDateFormat(
	    "yyyymmddhhmmssSSS");
    public static Job job;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	conf = new Configuration();
	conf.setInt("CandleLength", Integer.parseInt(args[0]) * 1000);
	fs = FileSystem.get(conf);
	Path p = new Path(args[2]);
	// fs.delete(p, true);
	job = new Job(conf, "candles");
	job.setJarByClass(Candles.class);
	job.setMapOutputKeyClass(Key.class);
	job.setMapOutputValueClass(Value.class);
	job.setOutputKeyClass(String.class);
	job.setOutputValueClass(String.class);
	job.setMapperClass(Map.class);
	job.setPartitionerClass(Part.class);
	job.setReducerClass(Reduce.class);
	job.setSortComparatorClass(Sort.class);
	job.setGroupingComparatorClass(Group.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, p);
	job.waitForCompletion(true);
    }
}