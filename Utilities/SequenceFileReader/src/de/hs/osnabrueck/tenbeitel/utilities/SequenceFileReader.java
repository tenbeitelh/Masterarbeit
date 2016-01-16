package de.hs.osnabrueck.tenbeitel.utilities;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
//import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
//import org.apache.mahout.math.NamedVector;

public class SequenceFileReader extends Configured implements Tool {
	private static final String LINE_DELIMITER = "\t";

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			int res = ToolRunner.run(new Configuration(), new SequenceFileReader(), args);
			System.exit(res);
		} else {
			System.out.println("No input path given. Cluster output folder is needed");
			System.exit(0);
		}
	}

	@Override
	public int run(String[] args) throws IOException {
		Configuration conf = this.getConf();

		Option filePath = SequenceFile.Reader.file(new Path(args[0]));
		try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, filePath)) {

			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

			if (key instanceof WeightedPropertyVectorWritable) {
				System.out.println("twitter_id" + LINE_DELIMITER + "cluster_id");
				while (reader.next(key, value)) {
					NamedVector nVector = (NamedVector) ((WeightedPropertyVectorWritable) value).getVector();
					System.out.println(nVector.getName() + " belongs to cluster " + key.toString());
				}
			} else {

				int recodCounter = 0;
				while (reader.next(key, value)) {

					recodCounter++;
					System.out.println(key.toString() + "\t" + value.toString());

				}
				System.out.println("Founrd " + recodCounter + "similar twitter messages");
			}
		}

		return 1;
	}

}
