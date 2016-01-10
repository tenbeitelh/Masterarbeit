package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

public class InformationflowMapper extends Mapper<Object, Object, Object, Object> {

	@Override
	protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
		System.out.println("KEY[class:[" + key.getClass().getName() + "]]:[" + key.toString() + "]VALUE[class:"
				+ value.getClass().getName() + "]:[" + value.toString() + "]");
	}

}
