package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.util.Arrays;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

public class Test {

	private static DefaultDirectedGraph<String, DefaultEdge> graph = new CustomDefaultDirectedGraph<String, DefaultEdge>(
			DefaultEdge.class);

	public static void main(String[] args) {
		for (int i = 1; i < 10; i++) {
			graph.addVertex(String.valueOf(i));
		}

		graph.addEdge("1", "3");
		graph.addEdge("1", "4");
		graph.addEdge("2", "3");
		graph.addEdge("5", "9");

		System.out.println(graph);

		System.out.println(graph.edgeSet());
		System.out.println(graph.vertexSet());

		String vertexString = graph.vertexSet().toString();
		String edgeString = graph.edgeSet().toString();

		String tmp = vertexString;
		tmp = tmp.replaceAll("\\[", "");
		tmp = tmp.replaceAll("\\]", "");
		tmp = tmp.replaceAll("\\s+", "");
		String[] vertexs = tmp.split(",");

		System.out.println(Arrays.toString(vertexs));
		System.out.println();

		tmp = edgeString;
		tmp = tmp.replaceAll("\\[", "");
		tmp = tmp.replaceAll("\\]", "");
		tmp = tmp.replaceAll("\\s+", "");
		tmp = tmp.replaceAll("\\(", "");
		tmp = tmp.replaceAll("\\)", "");

		String[] edges = tmp.split(",");

		System.out.println(Arrays.toString(edges));

		System.out.println();
		System.out.println(createGraph(vertexs, edges));

	}

	private static DefaultDirectedGraph<String, DefaultEdge> createGraph(String[] vertexArray, String[] edgeArray) {
		DefaultDirectedGraph<String, DefaultEdge> graph = new DefaultDirectedGraph<String, DefaultEdge>(
				DefaultEdge.class);
		for (String vertex : vertexArray) {
			graph.addVertex(vertex);
		}
		for (String edge : edgeArray) {
			String parts[] = edge.split(":");
			graph.addEdge(parts[0], parts[1]);
		}

		return graph;
	}
}
