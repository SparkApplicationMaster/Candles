package candles;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator {

    public Group() {
	super(Key.class, true);
    }

    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable k1, WritableComparable k2) {
	Key key1 = (Key) k1;
	Key key2 = (Key) k2;
	int x = key1.instrument.compareTo(key2.instrument);
	if (x != 0) {
	    return x;
	} else {
	    x = (int) (key1.time / key1.candleLength - key2.time
		    / key2.candleLength);
	    return x;
	}
    }
}
