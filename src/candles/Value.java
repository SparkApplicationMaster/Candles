package candles;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Value implements Writable {
    float cost;
    int volume;

    public Value() {
    };

    public Value(float cos, int vol) {
	cost = cos;
	volume = vol;

    }

    @Override
    public void readFields(DataInput input) throws IOException {
	cost = input.readFloat();
	volume = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
	output.writeFloat(cost);
	output.writeInt(volume);
    }
}
