package de.hs.osnabrueck.tenbeitel.mr.informationflow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class InformationflowCombiner extends Reducer<Text, Text, Text, Text> {

	private static DefaultDirectedGraph<String, DefaultEdge> graph;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		InformationflowCombiner.graph = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		String graphAsString = GraphUtils.getStringRepresantationFromGraph(graph);
		System.out.println(graphAsString);

		context.write(context.getCurrentKey(), new Text(graphAsString));

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String statusIdString = key.toString();
		InformationflowCombiner.graph.addVertex(statusIdString);

		for (Text repliedBy : values) {
			String repliedByString = repliedBy.toString();
			InformationflowCombiner.graph.addVertex(repliedByString);
			InformationflowCombiner.graph.addEdge(repliedByString, statusIdString);
		}

	}

}
