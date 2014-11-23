package candles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.WritableComparable;

public class Key implements WritableComparable<Key> {
    String instrument = new String();
    long time;
    int candleLength;

    public Key() {
    }

    Key(String instr, long tim, int candll) throws ParseException {
	instrument = instr;
	time = tim;
	candleLength = candll;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
	instrument = input.readUTF();
	time = input.readLong();
	candleLength = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
	output.writeUTF(instrument);
	output.writeLong(time);
	output.writeInt(candleLength);
    }

    @Override
    public int compareTo(Key o) {
	int diff = instrument.compareTo(o.instrument);
	if (diff != 0) {
	    return diff;
	}
	return (int) (time - o.time);
    }

}
