package de.hs.osnabrueck.hadoop.util;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopPathUtils {
	public static void deletePathIfExists(Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}
