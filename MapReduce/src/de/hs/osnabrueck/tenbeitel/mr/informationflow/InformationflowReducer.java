package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.google.gson.Gson;

public class InformationflowReducer extends Reducer<Text, Text, Text, Text> {
	private static final Gson GSON = new Gson();

	private static Graph<String, DefaultEdge> graph = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text graphString : values) {
			@SuppressWarnings("unchecked")
			Graph<String, DefaultEdge> actualGraph = GSON.fromJson(graphString.toString(), Graph.class);

			Graphs.addGraph(InformationflowReducer.graph, actualGraph);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		String graphJson = GSON.toJson(graph);
		context.write(context.getCurrentKey(), new Text(graphJson));
	}

}
