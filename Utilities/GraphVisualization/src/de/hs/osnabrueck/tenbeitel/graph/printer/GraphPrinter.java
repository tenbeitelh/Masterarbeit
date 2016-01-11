package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.swing.JFrame;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.mxgraph.model.mxIGraphModel;
import com.mxgraph.view.mxGraph;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;

public class GraphPrinter extends JFrame {
	private JGraphXAdapter<String, DefaultEdge> jgxAdapter;
	
	public static void main(String[] args) {

		JFrame f = new JFrame();
		f.setSize(800, 800);
		f.setLocation(300, 200);
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		DefaultDirectedGraph<String, DefaultEdge> graph = null;
		try {
			String graphString = readFile(args[0]);
			graph = GraphUtils.getGraphFromString(graphString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(graph == null){
			System.out.println("Graph could not be read");
			System.exit(ERROR);
		}
		final mxGraph graphView = new mxGraph();
		

	}

	private static String readFile(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded, StandardCharsets.UTF_8);
	}
}
