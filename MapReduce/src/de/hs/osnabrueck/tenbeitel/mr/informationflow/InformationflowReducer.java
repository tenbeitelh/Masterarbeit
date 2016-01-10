package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class InformationflowReducer extends Reducer<Writable, Writable, Writable, Writable> {

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

}
