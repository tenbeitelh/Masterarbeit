package de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class ExtendGraphReducer extends Reducer<Text, Text, NullWritable, Text> {
	private static DefaultDirectedGraph<String, DefaultEdge> extendedGraph = new DefaultDirectedGraph<String, DefaultEdge>(
			DefaultEdge.class);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text graph : values) {
			DefaultDirectedGraph<String, DefaultEdge> actualGraph = GraphUtils.getGraphFromString(graph.toString());
			System.out.println(extendedGraph.vertexSet().toString());
			System.out.println();
			Graphs.addGraph(extendedGraph, actualGraph);
			System.out.println(actualGraph.vertexSet().toString());
			System.out.println(extendedGraph.vertexSet().toString());
			System.out.println();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		String extendedGraphString = GraphUtils.getStringRepresantationFromGraph(extendedGraph);
		context.write(NullWritable.get(), new Text(extendedGraphString));

	}

}
