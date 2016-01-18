package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import javax.swing.JFrame;

import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.ListenableDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;

import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxFastOrganicLayout;
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

		JFrame frame = new JFrame();
		frame.setSize(1000, 1000);
		frame.setLocation(300, 200);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JFrame frame2 = new JFrame();
		frame2.setSize(1000, 1000);
		frame2.setLocation(300, 200);
		frame2.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		ListenableDirectedGraph<String, DefaultEdge> graph = null;
		ListenableDirectedGraph<String, DefaultEdge> lSubGraph = null;
		try {
			// String graphString = readFile(
			// "C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\graph_data_20160113");
			String graphString = readFile(
					"C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\graph_data_extended_20160118");
			graph = new ListenableDirectedGraph<String, DefaultEdge>(GraphUtils.getGraphFromString(graphString));

			// ConnectivityInspector<String, DefaultEdge> inspector = new
			// ConnectivityInspector<String, DefaultEdge>(
			// graph);
			//
			// List<Set<String>> cSet = inspector.connectedSets();
			//
			// int max = 0;
			// int pos = 0;
			//
			// for (int i = 0; i < cSet.size(); i++) {
			// if (cSet.get(i).size() > max) {
			// max = cSet.get(i).size();
			// pos = i;
			// if (max > 9) {
			// break;
			// }
			// }
			//
			// }
			// System.out.println("Max: " + max + " found on position " + pos);
			//
			// // DirectedSubgraph<String, DefaultEdge> subGraph = new
			// // DirectedSubgraph<>(graph,
			// // inspector.connectedSetOf("681806320769601536"), null);
			//
			// DirectedSubgraph<String, DefaultEdge> subGraph = new
			// DirectedSubgraph<>(graph, cSet.get(pos), null);
			//
			// // DepthFirstIterator<String, DefaultEdge> iterator = new
			// // DepthFirstIterator<String, DefaultEdge>(graph);
			// // DirectedSubgraph<String, DefaultEdge> subGraph = new
			// // DirectedSubgraph<>(graph,
			// // inspector.connectedSetOf(iterator.next()), null);
			// lSubGraph = new ListenableDirectedGraph<String,
			// DefaultEdge>(subGraph);

		} catch (IOException e) {
			e.printStackTrace();
		}

		if (graph == null) {
			System.out.println("Graph could not be read");
			System.exit(ERROR);
		}

		// jgxAdapter = new JGraphXAdapter<String, DefaultEdge>(lSubGraph);
		// jgxAdapter.getStylesheet().getDefaultEdgeStyle().put(mxConstants.STYLE_NOLABEL,
		// "1");

		// mxGraphComponent graphComponent = new mxGraphComponent(jgxAdapter);
		// graphComponent.setConnectable(false);
		// graphComponent.setEnabled(false);
		//
		// mxHierarchicalLayout layout = new mxHierarchicalLayout(jgxAdapter);
		// layout.setDisableEdgeStyle(true);
		// layout.execute(jgxAdapter.getDefaultParent());
		//
		// frame.getContentPane().add(graphComponent);

		jgxAdapter = new JGraphXAdapter<String, DefaultEdge>(graph);
		jgxAdapter.getStylesheet().getDefaultEdgeStyle().put(mxConstants.STYLE_NOLABEL, "1");

		mxGraphComponent graphComponent2 = new mxGraphComponent(jgxAdapter);
		graphComponent2.setConnectable(false);
		graphComponent2.setEnabled(false);
		graphComponent2.setDragEnabled(true);

		new mxHierarchicalLayout(jgxAdapter).execute(jgxAdapter.getDefaultParent());
		frame2.getContentPane().add(graphComponent2);

		// frame.pack();
		// frame.setVisible(true);
		frame2.pack();
		frame2.setVisible(true);

	}

	private static String readFile(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded, StandardCharsets.UTF_8);
	}
}
