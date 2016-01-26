package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.ListenableDirectedGraph;

import de.hs.osnabrueck.tenbeitel.graph.printer.utils.DrawGraphUtils;
import de.hs.osnabrueck.tenbeitel.twitter.graph.model.TwitterVertex;

public class GraphOfMonth {
	public static void main(String[] args) throws IOException {
		DirectedGraph<TwitterVertex, DefaultEdge> init_graph = DrawGraphUtils.getGraphFromFile(
				"C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\20160125_init_graph");

		ConnectivityInspector<TwitterVertex, DefaultEdge> inspector = new ConnectivityInspector<>(init_graph);

		System.out.println("#Vertex in graph: " + init_graph.vertexSet().size());
		System.out.println("Connected sets: " + inspector.connectedSets().size());

		int max = 0;
		int countNotAvailbe = 0;

		Map<Integer, AtomicInteger> countMap = new HashMap<Integer, AtomicInteger>();
		for (Set<TwitterVertex> set : inspector.connectedSets()) {
			if (countMap.containsKey(set.size())) {
				countMap.get(set.size()).getAndIncrement();
			} else {
				countMap.put(set.size(), new AtomicInteger(1));
			}
			if (set.size() > max) {
				max = set.size();
			}
			for (TwitterVertex vertex : set) {
				if (vertex.getUserId().equalsIgnoreCase("N/A")) {
					countNotAvailbe++;
				}
			}

			if (set.size() > 10) {
				System.out.println(inspector.connectedSets().indexOf(set));
			}
		}

		System.out.println("Max: " + max);
		System.out.println("Not available count: " + countNotAvailbe);

		for (Integer key : countMap.keySet()) {
			System.out.println("#Elements: " + key + " in sets: " + countMap.get(key));
		}

		DrawGraphUtils.drawGraphsForIds(init_graph, inspector, 375, 594);

		// DirectedGraph<TwitterVertex, DefaultEdge> extGraph =
		// DrawGraphUtils.getGraphFromFile(
		// "C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\20160125_ext_graph");
		//
		// List<TwitterVertex> interestedVertex = getVertexForUserId(extGraph,
		// "674521645495754752", "673415714959712256");
		//
		// ConnectivityInspector<TwitterVertex, DefaultEdge> extInspector = new
		// ConnectivityInspector<>(init_graph);
		//
		// for(TwitterVertex vertex : interestedVertex){
		// Set<TwitterVertex> subGraphSet = extInspector.connectedSetOf(vertex);
		// DrawGraphUtils.drawTwitterGraph(new ListenableDirectedGraph<>(
		// new DirectedSubgraph<>(extGraph, subGraphSet, null)));
		// }

		//

	}

	private static List<TwitterVertex> getVertexForUserId(DirectedGraph<TwitterVertex, DefaultEdge> extGraph,
			String... twitterIds) {
		List<TwitterVertex> temp = new ArrayList<TwitterVertex>();
		for (TwitterVertex vertex : extGraph.vertexSet()) {
			for (String twitterId : twitterIds) {
				if (vertex.getTwitterId().equalsIgnoreCase(twitterId)) {
					temp.add(vertex);
				}
			}
		}
		return temp;
	}
}
