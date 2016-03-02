package de.hs.osnabrueck.tenbeitel.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RecordCounter extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RecordCounter(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		Path inputPath = new Path(args[0], "part-[mr]-[0-9]*");

		FileSystem fs = FileSystem.get(conf);

		FileStatus[] files = fs.globStatus(inputPath);
		int recodCounter = 0;
		for (FileStatus file : files) {
			Option fileOption = SequenceFile.Reader.file(file.getPath());

			try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption)) {
				Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

				while (reader.next(key, value)) {
					recodCounter++;
				}
			}

		}
		System.out.println("#Transactions in path: " + args[0] + " - " + recodCounter);

		return 0;
	}

}
