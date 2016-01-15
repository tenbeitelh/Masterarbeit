package de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.Vector;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class ExtendGraphReducer extends Reducer<IntWritable, WeightedPropertyVectorWritable, Text, Text> {
	private static DirectedGraph<String, DefaultEdge> graph;

	private static Map<String, Vector> vectorMap;
	private static Map<String, String> clusterIdToTwitteridsMap;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		readGraphFromCachedFile(context);
		vectorMap = new HashMap<String, Vector>();
		clusterIdToTwitteridsMap = new HashMap<String, String>();
	}

	@Override
	protected void reduce(IntWritable key, Iterable<WeightedPropertyVectorWritable> values, Context context)
			throws IOException, InterruptedException {

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);

	}

	private void extendInitialGraph(Context context) {
		ConnectivityInspector<String, DefaultEdge> inspector = new ConnectivityInspector<String, DefaultEdge>(graph);
		List<Set<String>> connectedSet = inspector.connectedSets();

		for (int subGraphPos = 0; subGraphPos < connectedSet.size(); subGraphPos++) {
			List<String> subGraphVertexs = new ArrayList<String>(connectedSet.get(subGraphPos));
			for (int vertexPos = 0; vertexPos < subGraphVertexs.size(); vertexPos++) {
				if (clusterIdToTwitteridsMap.values().contains(subGraphVertexs.get(vertexPos))) {
					// List<String> idsToProcess =
					// getNotIncludedIdsFromCluster();
				}
			}
		}
	}

	private void readGraphFromCachedFile(Context context) throws IOException {
		URI[] cachedFiles = context.getCacheFiles();
		if (cachedFiles != null && cachedFiles.length > 0) {
			File graphFile = new File(cachedFiles[0]);
			String graphString = FileUtils.readFileToString(graphFile);
			graph = GraphUtils.getGraphFromString(graphString);
		}
	}

	@SuppressWarnings("unused")
	private void createSubGraphList(List<DirectedSubgraph<String, DefaultEdge>> subGraphList) {
		ConnectivityInspector<String, DefaultEdge> inspector = new ConnectivityInspector<String, DefaultEdge>(graph);
		List<Set<String>> connectedSet = inspector.connectedSets();
		for (int connectedSetCounter = 0; connectedSetCounter < connectedSet.size(); connectedSetCounter++) {
			subGraphList.add(new DirectedSubgraph<>(graph, connectedSet.get(connectedSetCounter), null));
		}
	}

}
