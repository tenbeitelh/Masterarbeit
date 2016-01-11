package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.swing.JFrame;

import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.ListenableDirectedGraph;

import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxCompactTreeLayout;
import com.mxgraph.layout.mxFastOrganicLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.layout.mxParallelEdgeLayout;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.swing.util.mxMorphing;
import com.mxgraph.util.mxEvent;
import com.mxgraph.util.mxEventObject;
import com.mxgraph.util.mxEventSource.mxIEventListener;
import com.mxgraph.view.mxGraph;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class GraphPrinter extends JFrame {

	private static JGraphXAdapter<String, DefaultEdge> jgxAdapter;

	public static void main(String[] args) {

		JFrame f = new JFrame();
		f.setSize(800, 800);
		f.setLocation(300, 200);
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		ListenableDirectedGraph<String, DefaultEdge> graph = null;
		try {
			String graphString = readFile("C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\Graph");
			graph = new ListenableDirectedGraph<String, DefaultEdge>(GraphUtils.getGraphFromString(graphString));

			ConnectivityInspector<String, DefaultEdge> inspector = new ConnectivityInspector<String, DefaultEdge>(
					graph);

			List<Set<String>> cSet = inspector.connectedSets();

			int max = 0;
			int pos = 0;

			for (int i = 0; i < cSet.size(); i++) {
				if (cSet.get(i).size() > max) {
					max = cSet.get(i).size();
					pos = i;
				}
			}
			System.out.println("Max: " + max + " found on position " + pos);

			DirectedSubgraph<String, DefaultEdge> subGraph = new DirectedSubgraph<>(graph,
					inspector.connectedSets().get(pos), null);

			graph = new ListenableDirectedGraph<String, DefaultEdge>(subGraph);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (graph == null) {
			System.out.println("Graph could not be read");
			System.exit(ERROR);
		}

		jgxAdapter = new JGraphXAdapter<String, DefaultEdge>(graph);

		mxGraphComponent graphComponent = new mxGraphComponent(jgxAdapter);
		graphComponent.setConnectable(false);
		graphComponent.setEnabled(false);

		mxCircleLayout layout = new mxCircleLayout(jgxAdapter);
		layout.execute(jgxAdapter.getDefaultParent());

		f.getContentPane().add(graphComponent);

		f.pack();
		f.setVisible(true);

	}

	private static String readFile(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded, StandardCharsets.UTF_8);
	}
}
