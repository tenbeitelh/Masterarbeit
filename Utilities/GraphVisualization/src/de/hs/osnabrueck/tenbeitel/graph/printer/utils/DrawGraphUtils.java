package de.hs.osnabrueck.tenbeitel.graph.printer.utils;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import javax.imageio.ImageIO;
import javax.swing.JFrame;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.ListenableDirectedGraph;

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.util.mxCellRenderer;
import com.mxgraph.util.mxConstants;
import com.mxgraph.util.mxUtils;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;
import de.hs.osnabrueck.tenbeitel.twitter.graph.model.TwitterVertex;

public class DrawGraphUtils {
	public static void drawTwitterGraph(ListenableDirectedGraph<TwitterVertex, DefaultEdge> graph) {
		JFrame frame = new JFrame();
		frame.setSize(1000, 1000);
		frame.setLocation(300, 200);
		// frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JGraphXAdapter jgxAdapter = new JGraphXAdapter<TwitterVertex, DefaultEdge>(graph);
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

	public static DirectedGraph<TwitterVertex, DefaultEdge> getGraphFromFile(String path) throws IOException {
		String graphAsString = readFile(path);
		return GraphUtils.createTwitterGraphFromString(graphAsString);
	}

	private static String readFile(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded, StandardCharsets.UTF_8);
	}

	public static void drawGraphsForIds(DirectedGraph<TwitterVertex, DefaultEdge> graph,
			ConnectivityInspector<TwitterVertex, DefaultEdge> inspector, int... ids) {
		for (int id : ids) {
			drawTwitterGraph(new ListenableDirectedGraph<>(
					new DirectedSubgraph<>(graph, inspector.connectedSets().get(id), null)));
		}
	}

	public static void saveGraphPNG(DirectedGraph<TwitterVertex, DefaultEdge> graph, String dir) throws IOException {
		int counter = 0;
		ConnectivityInspector<TwitterVertex, DefaultEdge> inspector = new ConnectivityInspector<>(graph);

		for (Set<TwitterVertex> set : inspector.connectedSets()) {

			ListenableDirectedGraph<TwitterVertex, DefaultEdge> tree = new ListenableDirectedGraph<>(
					new DirectedSubgraph<>(graph, set, null));
			JGraphXAdapter jgxAdapter = new JGraphXAdapter<TwitterVertex, DefaultEdge>(tree);
			
			jgxAdapter.getStylesheet().getDefaultEdgeStyle().put(mxConstants.STYLE_NOLABEL, "1");
			jgxAdapter.getStylesheet().getDefaultVertexStyle().put(mxConstants.STYLE_FILLCOLOR,
					mxUtils.getHexColorString(Color.WHITE));
			jgxAdapter.getStylesheet().getDefaultVertexStyle().put(mxConstants.STYLE_FONTCOLOR,
					mxUtils.getHexColorString(Color.BLACK));

			mxGraphComponent graphComponent2 = new mxGraphComponent(jgxAdapter);
			graphComponent2.setConnectable(false);
			graphComponent2.setEnabled(false);
			graphComponent2.setDragEnabled(true);

			new mxHierarchicalLayout(jgxAdapter).execute(jgxAdapter.getDefaultParent());
			String filename = dir + "\\graph_" + counter++ + ".png";
			System.out.format("Saving subgraph to file: %s \n", filename);
			BufferedImage image = mxCellRenderer.createBufferedImage(jgxAdapter, null, 1, Color.WHITE, true, null);

			ImageIO.write(image, "PNG", new File(filename));
		}

	}
}
