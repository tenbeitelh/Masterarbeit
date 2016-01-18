package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import javax.xml.transform.TransformerConfigurationException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.alg.EdmondsKarpMaximumFlow;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.GraphMLExporter;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.ext.VisioExporter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.ListenableDirectedGraph;
import org.xml.sax.SAXException;

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.util.mxConstants;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class GraphPrinter extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7296374538320083805L;

	private static JGraphXAdapter<String, DefaultEdge> jgxAdapter;

	public static void main(String[] args) {

		JFrame frame2 = new JFrame();
		frame2.setSize(1000, 1000);
		frame2.setLocation(300, 200);
		frame2.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		ListenableDirectedGraph<String, DefaultEdge> graph = null;

		try {
			String graphString = readFile(
					"C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\grah_data");
			// String graphString = readFile(
			// "C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\graph_data_extended_20160118");
			graph = new ListenableDirectedGraph<String, DefaultEdge>(GraphUtils.getGraphFromString(graphString));
			// drawGraph(graph);
			// exportDOT(graph);
			// exportGraphML(graph);
			// exportGraphVisio(graph);
			// graphWithCycles(graph);

		} catch (IOException e) {
			e.printStackTrace();
		}

		if (graph == null) {
			System.out.println("Graph could not be read");
			System.exit(ERROR);
		}

		// SwingUtilities.invokeLater(new Runnable() {
		//
		// @Override
		// public void run() {
		// drawGraph(graph);
		// }
		// });

		System.out.println("Number vertex: " + graph.vertexSet().size());
		System.out.println("Number edges: " + graph.edgeSet().size());

		ConnectivityInspector<String, DefaultEdge> inspector = new ConnectivityInspector<String, DefaultEdge>(graph);

		List<Set<String>> cSet = inspector.connectedSets();

		System.out.println("# Subgrahps: " + cSet.size());

		for (int i = 0; i < cSet.size(); i++) {
			System.out.println("Pos: " + i + " #Vertexes:" + cSet.get(i).size());
		}
		
		
		
		DirectedSubgraph<String, DefaultEdge> subGraph = new DirectedSubgraph<>(graph, cSet.get(0), null);
		final ListenableDirectedGraph<String, DefaultEdge> lSubGraph = new ListenableDirectedGraph<String, DefaultEdge>(
				subGraph);
		
		graph(graph);
		
		// SwingUtilities.invokeLater(new Runnable() {
		//
		// @Override
		// public void run() {
		// drawGraph(lSubGraph);
		// }
		// });

	}

	protected static void drawGraph(ListenableDirectedGraph<String, DefaultEdge> graph) {
		JFrame frame = new JFrame();
		frame.setSize(1000, 1000);
		frame.setLocation(300, 200);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		jgxAdapter = new JGraphXAdapter<String, DefaultEdge>(graph);
		jgxAdapter.getStylesheet().getDefaultEdgeStyle().put(mxConstants.STYLE_NOLABEL, "1");

		mxGraphComponent graphComponent2 = new mxGraphComponent(jgxAdapter);
		graphComponent2.setConnectable(false);
		graphComponent2.setEnabled(false);
		graphComponent2.setDragEnabled(true);

		new mxHierarchicalLayout(jgxAdapter).execute(jgxAdapter.getDefaultParent());
		frame.getContentPane().add(graphComponent2);

		frame.pack();
		frame.setVisible(true);

	}

	private static String readFile(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded, StandardCharsets.UTF_8);
	}

	private static void exportDOT(ListenableDirectedGraph<String, DefaultEdge> graph) throws IOException {
		DOTExporter<String, DefaultEdge> export = new DOTExporter<String, DefaultEdge>();
		export.export(new FileWriter("graph.dot"), graph);
	}

	private static void exportGraphML(ListenableDirectedGraph<String, DefaultEdge> graph)
			throws IOException, TransformerConfigurationException, SAXException {
		GraphMLExporter<String, DefaultEdge> exporter = new GraphMLExporter<String, DefaultEdge>();
		exporter.export(new FileWriter("grah_ml.graphml"), graph);
	}

	private static void exportGraphVisio(ListenableDirectedGraph<String, DefaultEdge> graph) throws IOException {
		VisioExporter<String, DefaultEdge> exporter = new VisioExporter<String, DefaultEdge>();
		exporter.export(new FileOutputStream(new File("visio.vsd")), graph);
	}

	private static void graphWithCycles(ListenableDirectedGraph<String, DefaultEdge> graph) {
		CycleDetector<String, DefaultEdge> detector = new CycleDetector<String, DefaultEdge>(graph);
		System.out.println(detector.detectCycles());
		Set<String> set = detector.findCycles();
		for (String cycle : set) {
			System.out.println(cycle);
		}

		DirectedSubgraph<String, DefaultEdge> subGraph = new DirectedSubgraph<>(graph, set, null);
		ListenableDirectedGraph<String, DefaultEdge> lSubGraph = new ListenableDirectedGraph<>(subGraph);
		drawGraph(lSubGraph);

	}
	
	private static void graph(ListenableDirectedGraph<String, DefaultEdge> graph){
		StrongConnectivityInspector<String, DefaultEdge> subGraph = new StrongConnectivityInspector<>(graph);
		List<DirectedSubgraph<String, DefaultEdge>> graphs = subGraph.stronglyConnectedSubgraphs();
		
		for(int i = 0; i < graphs.size(); i++){
			System.out.println(i + " - " + graphs.get(i).vertexSet().size());
		}
		
		
	}
	
	private static void max(ListenableDirectedGraph<String, DefaultEdge> graph){
		
	}
}
