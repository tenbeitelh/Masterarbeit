package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class InformationflowMapper extends Mapper<Writable, Writable, Writable, Writable> {

	@Override
	protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
		System.out.println("KEY[class:[" + key.getClass().getName() + "]]:[" + key.toString() + "]VALUE[class:"
				+ value.getClass().getName() + "]:[" + value.toString() + "]");
	}

}
