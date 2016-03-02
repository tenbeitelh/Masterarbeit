package de.hs.osnabrueck.tenbeitel.mr.informationflow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class InformationflowReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static DefaultDirectedGraph<String, DefaultEdge> graph = new DefaultDirectedGraph<String, DefaultEdge>(
			DefaultEdge.class);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text graphText : values) {
			Graphs.addGraph(graph, GraphUtils.getGraphFromString(graphText.toString()));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		String graphAsString = GraphUtils.getStringRepresantationFromGraph(graph);
		System.out.println(graphAsString);

		context.write(NullWritable.get(), new Text(graphAsString));
		
	}

}
