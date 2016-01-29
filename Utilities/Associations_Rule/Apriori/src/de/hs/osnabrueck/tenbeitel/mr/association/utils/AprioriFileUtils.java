package de.hs.osnabrueck.tenbeitel.mr.association.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.mahout.common.StringTuple;
import org.apache.hadoop.io.Writable;

public class AprioriFileUtils {
	public static boolean previousItemSetsExists(Configuration conf, Path previousItemSetsFolder) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		Path globPattern = new Path(previousItemSetsFolder, "part-r-[0-9]*");

		FileStatus[] reducerFiles = fs.globStatus(globPattern);
		int itemSetCounter = 0;
		for (FileStatus reducerFile : reducerFiles) {
			Option fileOption = SequenceFile.Reader.file(reducerFile.getPath());
			try (SequenceFile.Reader frequentItemsReader = new SequenceFile.Reader(conf, fileOption)) {
				Writable key = new StringTuple();
				Writable value = new IntWritable();
				while (frequentItemsReader.next(key, value)) {
					itemSetCounter++;
					if (itemSetCounter > 1) {
						return true;
					}
				}
			}
		}

		return false;
	}
}
