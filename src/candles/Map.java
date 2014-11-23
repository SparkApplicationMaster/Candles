package candles;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Key, Value> {
    public static DateFormat dateFormat = new SimpleDateFormat(
	    "yyyymmddhhmmssSSS");

    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	String[] words = value.toString().split(",");
	Configuration conf = context.getConfiguration();
	int candleLength = conf.getInt("CandleLength", 1);
	try {
	    Date date = dateFormat.parse(words[2]);
	    long seconds = date.getTime();
	    context.write(new Key(words[0], seconds, candleLength), new Value(
		    Float.parseFloat(words[4]), Integer.parseInt(words[5])));
	} catch (Exception e) {
	}

    }
}