package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import org.jgrapht.EdgeFactory;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

public class CustomDefaultDirectedGraph<V, E> extends DefaultDirectedGraph<V, E> {
	private static final Class EDGE_CLASS = DefaultEdge.class;

	public CustomDefaultDirectedGraph() {
		super(new ClassBasedEdgeFactory<V, E>(EDGE_CLASS));
	}

	public CustomDefaultDirectedGraph(Class edgeClass) {
		super(edgeClass);
	}

	public CustomDefaultDirectedGraph(EdgeFactory ef) {
		super(ef);
	}

}
