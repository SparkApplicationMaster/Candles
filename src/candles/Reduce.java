package candles;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reduce extends Reducer<Key, Value, String, String> {
    public static DateFormat dateFormat = new SimpleDateFormat(
	    "yyyymmddhhmmssSSS");
    MultipleOutputs<String, String> multipleOutputs;
    static boolean newName;
    static String prevName = "";

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {

	multipleOutputs = new MultipleOutputs<String, String>(context);
    }

    public void reduce(Key key, Iterable<Value> values, Context context)
	    throws IOException, InterruptedException {
	Date date = new Date();
	float min = Float.MAX_VALUE, max = Float.MIN_VALUE, first = -1, last = -1;
	int volume = 0;

	date.setTime(key.time - key.time % key.candleLength);
	if (key.instrument.compareTo(prevName) != 0) {
	    newName = true;
	    prevName = key.instrument;
	} else {
	    newName = false;
	}
	if (newName) {
	    multipleOutputs.write("TIME,OPEN,HIGH,LOW,CLOSE,VOLUME", "",
		    "candles_" + key.instrument + ".csv");
	}

	for (Value value : values) {
	    volume += value.volume;
	    if (min > value.cost) {
		min = value.cost;
	    }
	    if (max < value.cost) {
		max = value.cost;
	    }
	    if (first == -1) {
		first = value.cost;
	    }
	    last = value.cost;

	}

	multipleOutputs.write(dateFormat.format(date) + "," + first + "," + max
		+ "," + min + "," + last + "," + volume, "", "candles_"
		+ key.instrument + ".csv");
    }

    @Override
    protected void cleanup(Context context) throws IOException,
	    InterruptedException {

	multipleOutputs.close();
    }
}
