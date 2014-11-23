package candles;

import org.apache.hadoop.mapreduce.Partitioner;

public class Part extends Partitioner<Key, Value> {

    @Override
    public int getPartition(Key key, Value value, int reducers) {
	if (reducers == 0) {
	    return 0;
	}
	return (key.instrument.hashCode() * (int) (key.time / key.candleLength))
		% reducers;
    }

}
