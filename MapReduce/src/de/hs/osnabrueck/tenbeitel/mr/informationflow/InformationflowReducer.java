package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class InformationflowReducer extends Reducer<Object, Object, Object, Object> {

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

}
