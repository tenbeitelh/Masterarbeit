package de.hs.osnabrueck.tenbeitel.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileReader extends Configured implements Tool {

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
	public int run(String[] args) {
		Configuration conf = this.getConf();

		String clusterOutput = args[0];

		Option filePath = SequenceFile.Reader.file(new Path(clusterOutput + "/clusteredPoints" + "/part-m-00000"));
		try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, filePath)) {

			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

			System.out.println(reader.getKeyClassName());
			System.out.println(reader.getValueClassName());

			while (reader.next(key, value)) {
				System.out.println(key.toString() + " belongs to cluster " + value.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

}
