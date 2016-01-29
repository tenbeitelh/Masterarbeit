package de.hs.osnabrueck.tenbeitel.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.StringTuple;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;
import de.hs.osnabrueck.tenbeitel.twitter.graph.constants.TwitterWritableConstants;
import de.hs.osnabrueck.tenbeitel.twitter.graph.model.TwitterVertex;

public class GraphToTransaction extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if (args.length > 1) {
			int res = ToolRunner.run(new Configuration(), new GraphToTransaction(), args);
			System.exit(res);
		} else {
			System.out.println("usage: [inputDir] [outputDir]");
			System.exit(0);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		Path graphPath = new Path(args[0]);

		FileSystem fs = FileSystem.get(conf);

		DefaultDirectedGraph<TwitterVertex, DefaultEdge> graph = GraphUtils
				.createTwitterGraphFromString(readHdfsFileToString(fs, graphPath));

		final ConnectivityInspector<TwitterVertex, DefaultEdge> connectivity = new ConnectivityInspector<TwitterVertex, DefaultEdge>(
				graph);

		List<StringTuple> userTuplesForSubgraphs = generateUserTuplesForSubgraphs(connectivity);

		List<StringTuple> directedUserTuplesForSubGraphs = generateDirectedUserTuplesForSubGraphs(graph, connectivity);

		return 0;
	}

	private List<StringTuple> generateDirectedUserTuplesForSubGraphs(
			DefaultDirectedGraph<TwitterVertex, DefaultEdge> baseGraph,
			ConnectivityInspector<TwitterVertex, DefaultEdge> connectivity) {
		for (Set<TwitterVertex> connectedSet : connectivity.connectedSets()) {
			DirectedSubgraph<TwitterVertex, DefaultEdge> subGraph = new DirectedSubgraph<>(baseGraph, connectedSet,
					null);
			
		}

		return null;
	}

	private List<StringTuple> generateUserTuplesForSubgraphs(
			ConnectivityInspector<TwitterVertex, DefaultEdge> connectivity) {
		List<StringTuple> tempTupleList = new ArrayList<StringTuple>();
		for (Set<TwitterVertex> connectedSet : connectivity.connectedSets()) {
			StringTuple tuple = new StringTuple();
			for (TwitterVertex vertex : connectedSet) {
				if (!vertex.getUserId().equalsIgnoreCase(TwitterWritableConstants.NOT_AVAILABLE)) {
					tuple.add(vertex.getUserId());
				}
			}
			tempTupleList.add(tuple);
		}
		return tempTupleList;
	}

	private String readHdfsFileToString(FileSystem fs, Path pathToFile) throws IOException {
		StringBuilder builder = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathToFile)))) {
			String line;
			line = br.readLine();
			while (line != null) {
				builder.append(line);
				line = br.readLine();
			}
		}
		return builder.toString();
	}

}
