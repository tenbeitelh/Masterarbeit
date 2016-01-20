package de.hs.osnabrueck.tenbeitel.mr.graph.utils;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import de.hs.osnabrueck.tenbeitel.twitter.graph.model.TwitterVertex;

public class GraphUtils {

	private static final String COMPONENT_DELIMITER = "\t";
	private static final String JSON_DELIMITER = "<__>";
	private static final String EDGE_DELIMITER = "<++>";

	public static String getStringRepresantationFromGraph(DefaultDirectedGraph<String, DefaultEdge> graph) {
		String vertexString = graph.vertexSet().toString();
		String edgeString = graph.edgeSet().toString();

		StringBuilder sb = new StringBuilder(vertexString);
		sb.append(COMPONENT_DELIMITER);
		sb.append(edgeString);

		return sb.toString();

	}

	public static String getStringRepresantationFromTwitterGraph(
			DefaultDirectedGraph<TwitterVertex, DefaultEdge> graph) {
		StringBuilder vertexStringBuilder = new StringBuilder();

		for (TwitterVertex vertex : graph.vertexSet()) {
			vertexStringBuilder.append(vertex.toJson());
			vertexStringBuilder.append(JSON_DELIMITER);
		}
		vertexStringBuilder.setLength(vertexStringBuilder.length() - 4);

		StringBuilder edgeStringBuilder = new StringBuilder();
		for (DefaultEdge edge : graph.edgeSet()) {
			edgeStringBuilder.append(graph.getEdgeSource(edge).toJson());
			edgeStringBuilder.append(EDGE_DELIMITER);
			edgeStringBuilder.append(graph.getEdgeTarget(edge).toJson());
			edgeStringBuilder.append(JSON_DELIMITER);
		}
		edgeStringBuilder.setLength(edgeStringBuilder.length() - 4);

		StringBuilder sb = new StringBuilder(vertexStringBuilder.toString());
		sb.append(COMPONENT_DELIMITER);
		sb.append(edgeStringBuilder.toString());

		return sb.toString();

	}

	public static DefaultDirectedGraph<TwitterVertex, DefaultEdge> createTwitterGraphFromString(
			String twitterGraphString) {
		DefaultDirectedGraph<TwitterVertex, DefaultEdge> twitterGraph = new DefaultDirectedGraph<TwitterVertex, DefaultEdge>(
				DefaultEdge.class);

		String[] vertexAndEdges = twitterGraphString.split(COMPONENT_DELIMITER);

		String[] jsonVertex = createJsonVertexArray(vertexAndEdges[0]);
		String[] edgeArray = createJsonEdgeArray(vertexAndEdges[1]);

		for (String vertex : jsonVertex) {
			System.out.println(vertex);
			twitterGraph.addVertex(TwitterVertex.fromJson(vertex));
		}

		for (String edges : edgeArray) {
			String[] edgeParts = edges.split(String.valueOf(EDGE_DELIMITER));
			TwitterVertex source = TwitterVertex.fromJson(edgeParts[0]);
			TwitterVertex target = TwitterVertex.fromJson(edgeParts[1]);
			twitterGraph.addEdge(source, target);
		}

		return twitterGraph;
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
		for (String edge : edgeArray) {
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

	private static String[] createJsonVertexArray(String vertexString) {
		return vertexString.split(String.valueOf(JSON_DELIMITER));
	}

	private static String[] createJsonEdgeArray(String edgeString) {
		// TODO Auto-generated method stub
		return edgeString.split(COMPONENT_DELIMITER);
	}

}
