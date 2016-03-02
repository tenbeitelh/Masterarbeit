package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;

import de.hs.osnabrueck.tenbeitel.graph.printer.utils.DrawGraphUtils;
import de.hs.osnabrueck.tenbeitel.twitter.graph.constants.TwitterWritableConstants;
import de.hs.osnabrueck.tenbeitel.twitter.graph.model.TwitterVertex;

public class GraphMetrics {
	static HashMap<String, String> metricsMap;

	public static void main(String[] args) throws IOException {
		String[] months = new String[] { "201510", "201511", "201512" };

		for (String month : months) {
			DirectedGraph<TwitterVertex, DefaultEdge> init_graph = DrawGraphUtils.getGraphFromFile(
					"C:\\development\\git_projects\\Masterarbeit\\Results\\Graphs\\twitter_graph_" + month);

			metricsMap = new HashMap<String, String>();

			int numberVertexs = init_graph.vertexSet().size();

			metricsMap.put("number_vertex", String.valueOf(numberVertexs));

			processGraph(init_graph);
			System.out.println(month);
			for (Map.Entry<String, String> entry : metricsMap.entrySet()) {
				System.out.println(entry.getKey() + ", " + entry.getValue());
			}
			System.out.println();

		}
	}

	private static void processGraph(DirectedGraph<TwitterVertex, DefaultEdge> init_graph) {
		// TODO Auto-generated method stub
		int countNa = 0;
		for (TwitterVertex vertex : init_graph.vertexSet()) {
			if (vertex.getClusterId().equals(TwitterWritableConstants.NOT_AVAILABLE)) {
				countNa++;
			}
		}

		metricsMap.put("count_na", String.valueOf(countNa));

		ConnectivityInspector<TwitterVertex, DefaultEdge> inspector = new ConnectivityInspector<>(init_graph);

		int countConnected = 0;
		int maxConnected = 0;

		for (Set<TwitterVertex> connectedSet : inspector.connectedSets()) {
			countConnected += connectedSet.size();
			if (maxConnected < connectedSet.size()) {
				maxConnected = connectedSet.size();
			}
		}

		metricsMap.put("count_connected", String.valueOf(countConnected));
		metricsMap.put("max_connected", String.valueOf(maxConnected));
		metricsMap.put("count_connected_sets", String.valueOf(inspector.connectedSets().size()));
	}

}
