package de.hs.osnabrueck.tenbeitel.mr.graph.utils;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

public class GraphUtils {

	public static final String COMPONENT_DELIMITER = "\t";

	public static String getStringRepresantationFromGraph(DefaultDirectedGraph<String, DefaultEdge> graph) {
		String vertexString = graph.vertexSet().toString();
		String edgeString = graph.edgeSet().toString();

		StringBuilder sb = new StringBuilder(vertexString);
		sb.append(COMPONENT_DELIMITER);
		sb.append(edgeString);

		return sb.toString();

	}

	public static DefaultDirectedGraph<String, DefaultEdge> getGraphFromString(String graphAsString) {
		String[] parts = graphAsString.split(COMPONENT_DELIMITER);

		return createGraph(createVertexArray(parts[0]), createEdgeArray(parts[1]));
	}

	private static DefaultDirectedGraph<String, DefaultEdge> createGraph(String[] vertexArray, String[] edgeArray) {
		DefaultDirectedGraph<String, DefaultEdge> graph = new DefaultDirectedGraph<String, DefaultEdge>(
				DefaultEdge.class);
		for (String vertex : vertexArray) {
			graph.addVertex(vertex);
		}
		for(String edge : edgeArray){
			String parts[] = edge.split(":");
			graph.addEdge(parts[0], parts[1]);
		}
		
		return graph;
	}

	private static String[] createVertexArray(String vertexString) {
		String tmp = vertexString.replaceAll("\\[", "");
		tmp = tmp.replaceAll("\\]", "");
		tmp = tmp.replaceAll("\\s+", "");

		return tmp.split(",");
	}

	private static String[] createEdgeArray(String edgeString) {

		String tmp = edgeString.replaceAll("\\[", "");
		tmp = tmp.replaceAll("\\]", "");
		tmp = tmp.replaceAll("\\s+", "");
		tmp = tmp.replaceAll("\\(", "");
		tmp = tmp.replaceAll("\\)", "");

		return tmp.split(",");
	}

}
