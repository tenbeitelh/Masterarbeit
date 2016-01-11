package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.google.gson.Gson;

public class InformationflowReducer extends Reducer<Text, BytesWritable, Text, Text> {
	private static final Gson GSON = new Gson();

	private static DefaultDirectedGraph<String, DefaultEdge> graph = new CustomDefaultDirectedGraph<String, DefaultEdge>(
			DefaultEdge.class);

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		for (BytesWritable graphString : values) {

			try {
				byte b[] = graphString.getBytes();
				ByteArrayInputStream bi = new ByteArrayInputStream(b);
				ObjectInputStream si = new ObjectInputStream(bi);
				DefaultDirectedGraph<String, DefaultEdge> actualGraph = (DefaultDirectedGraph<String, DefaultEdge>) si
						.readObject();
				System.out.println(GSON.toJson(actualGraph));
				Graphs.addGraph(InformationflowReducer.graph, actualGraph);
			} catch (Exception e) {
				System.out.println(e);
			}

		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		String graphJson = GSON.toJson(graph);
		context.write(context.getCurrentKey(), new Text(graphJson));
	}

}
