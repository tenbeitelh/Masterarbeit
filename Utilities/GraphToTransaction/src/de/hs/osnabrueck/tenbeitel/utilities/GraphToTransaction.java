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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
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

		List<StringTuple> clusterTuplesForSubgraphs = generateClusterTuplesForSubgraphs(connectivity);

		Path outputDir = new Path(args[1]);

		Path userPath = new Path(outputDir, "user_txn");
		Path clusterPath = new Path(outputDir, "cluster_txn");

		Path directedUserPath = new Path(outputDir, "directed_user_txn");

		writeTuples(conf, userPath, userTuplesForSubgraphs);
		writeTuples(conf, clusterPath, clusterTuplesForSubgraphs);
		writeTuples(conf, directedUserPath, directedUserTuplesForSubGraphs);

		return 0;
	}

	private void writeTuples(Configuration conf, Path userPath, List<StringTuple> userTuplesForSubgraphs)
			throws IOException {
		try (SequenceFile.Writer seqWriter = SequenceFile.createWriter(conf,
				Writer.file(new Path(userPath, "part-r-00000")), Writer.keyClass(NullWritable.class),
				Writer.valueClass(StringTuple.class))) {
			for (StringTuple tuple : userTuplesForSubgraphs) {
				seqWriter.append(NullWritable.get(), tuple);
			}
		}

	}

	private List<StringTuple> generateClusterTuplesForSubgraphs(
			ConnectivityInspector<TwitterVertex, DefaultEdge> connectivity) {
		List<StringTuple> clusterTuples = new ArrayList<StringTuple>();
		for (Set<TwitterVertex> connectedSet : connectivity.connectedSets()) {
			StringTuple transaction = new StringTuple();
			for (TwitterVertex vertex : connectedSet) {
				if (!vertex.getClusterId().equalsIgnoreCase(TwitterWritableConstants.NOT_AVAILABLE)) {
					transaction.add(vertex.getClusterId());
				}
			}
			clusterTuples.add(transaction);
		}
		return clusterTuples;
	}

	private List<StringTuple> generateDirectedUserTuplesForSubGraphs(
			DefaultDirectedGraph<TwitterVertex, DefaultEdge> baseGraph,
			ConnectivityInspector<TwitterVertex, DefaultEdge> connectivity) {
		List<StringTuple> tupleList = new ArrayList<StringTuple>();
		for (Set<TwitterVertex> connectedSet : connectivity.connectedSets()) {
			DirectedSubgraph<TwitterVertex, DefaultEdge> subGraph = new DirectedSubgraph<>(baseGraph, connectedSet,
					null);
			StringTuple transaction = new StringTuple();
			for (DefaultEdge edge : subGraph.edgeSet()) {
				TwitterVertex source = subGraph.getEdgeSource(edge);
				TwitterVertex target = subGraph.getEdgeTarget(edge);
				if (!source.getUserId().equalsIgnoreCase(TwitterWritableConstants.NOT_AVAILABLE)
						&& !target.getUserId().equalsIgnoreCase(TwitterWritableConstants.NOT_AVAILABLE)) {
					transaction.add(source.getUserId() + " --> " + target.getUserId());
				}
			}

			tupleList.add(transaction);
		}

		return tupleList;
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
