package de.hs.osnabrueck.tenbeitel.mr.twittergraph.reducer;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.io.TwitterWritable;

public class TwitterClusterJoinReducer extends Reducer<Text, TwitterWritable, NullWritable, Text> {
	
	DefaultDirectedGraph<String, DefaultEdge> idGraph;
	
	@Override
	protected void setup(Reducer<Text, TwitterWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		readGraphFromCachedFile(context);
	}

	@Override
	protected void reduce(Text key, Iterable<TwitterWritable> values, Context context)
			throws IOException, InterruptedException {
		
	}

	@Override
	protected void cleanup(Reducer<Text, TwitterWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	private void readGraphFromCachedFile(Context context) throws IOException {
		FileSystem fs = FileSystem.get(context.getConfiguration());

		URI[] cachedFiles = context.getCacheFiles();
		if (cachedFiles != null && cachedFiles.length > 0) {
			FSDataInputStream inputStream = fs.open(new Path(cachedFiles[0]));

			StringWriter writer = new StringWriter();
			IOUtils.copy(inputStream, writer, Charset.forName("UTF-8"));
			String graphString = writer.toString();
			// String graphString = FileUtils.readFileToString(graphFile);

			idGraph = GraphUtils.getGraphFromString(graphString);
		}
	}
}
