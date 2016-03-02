package de.hs.osnabrueck.tenbeitel.graph.printer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.ListenableDirectedGraph;

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.util.mxConstants;

import de.hs.osnabrueck.tenbeitel.mr.graph.utils.GraphUtils;
import de.hs.osnabrueck.tenbeitel.twitter.graph.model.TwitterVertex;

public class GraphObjectInspector {
	private static Map<String, String> userIdToName = new HashMap<String, String>();

	public static void main(String[] args) throws IOException {
		String twitterGraphString = readFile(
				"C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\twitter_graph");
		DefaultDirectedGraph<TwitterVertex, DefaultEdge> twitterGraph = GraphUtils
				.createTwitterGraphFromString(twitterGraphString);
		
		Set<TwitterVertex> vertexSet = twitterGraph.vertexSet();
		int count=0;
		for(TwitterVertex vertex : vertexSet){
			if(vertex.getUserId().equalsIgnoreCase("N/A")){
				count++;
			}
		}
		System.out.println(count);
		System.out.println(twitterGraph.vertexSet().size());
		System.out.println((new Double(count) / new Double(twitterGraph.vertexSet().size())) * 100);
		
		vertexSet = twitterGraph.vertexSet();
		
		Set<TwitterVertex> remove = new HashSet<TwitterVertex>();
		for(TwitterVertex vertex : vertexSet){
			if(vertex.getUserId().equalsIgnoreCase("N/A")){
				remove.add(vertex);
			}
		}
		
		ConnectivityInspector<TwitterVertex, DefaultEdge> inspector = new ConnectivityInspector<TwitterVertex, DefaultEdge>(
				twitterGraph);
		
		List<Set<TwitterVertex>> tempList2 = new ArrayList<Set<TwitterVertex>>();
		for(Set<TwitterVertex> set : inspector.connectedSets()){
			if(set.size() > 2){
				tempList2.add(set);
			}
		}
		
		twitterGraph.removeAllVertices(remove);
		
		ConnectivityInspector<TwitterVertex, DefaultEdge> inspector2 = new ConnectivityInspector<TwitterVertex, DefaultEdge>(
				twitterGraph);
		List<Set<TwitterVertex>> tempList = new ArrayList<Set<TwitterVertex>>();
		for(Set<TwitterVertex> set : inspector2.connectedSets()){
			if(set.size() > 2){
				tempList.add(set);
			}
			
		}
		
		
		
		System.out.println(tempList.size());
		System.out.println(tempList2.size());
		
		System.out.println(new Double(tempList.size()) / new Double(tempList2.size()) * 100);
	}
	
	public void test() throws IOException{
		String twitterGraphString = readFile(
				"C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\init_twitter");
		final DefaultDirectedGraph<TwitterVertex, DefaultEdge> twitterGraph2 = GraphUtils
				.createTwitterGraphFromString(twitterGraphString);

		twitterGraphString = readFile(
				"C:\\development\\git_projects\\Masterarbeit\\Utilities\\GraphAsTest\\twitter_graph");
		DefaultDirectedGraph<TwitterVertex, DefaultEdge> twitterGraph = GraphUtils
				.createTwitterGraphFromString(twitterGraphString);

		ConnectivityInspector<TwitterVertex, DefaultEdge> inspector2 = new ConnectivityInspector<TwitterVertex, DefaultEdge>(
				twitterGraph2);

		Set<TwitterVertex> vertxForUser = findVertexWithUser(twitterGraph2, "20393167");
		System.out.println(vertxForUser.size());
		for (final TwitterVertex userVertex : vertxForUser) {
			// SwingUtilities.invokeLater(new Runnable() {
			// public void run() {
			// drawTwitterGraph(new ListenableDirectedGraph<>(
			// new DirectedSubgraph<>(twitterGraph2,
			// inspector.connectedSetOf(userVertex), null)));
			// }
			// });

		}
		final ConnectivityInspector<TwitterVertex, DefaultEdge> inspector = new ConnectivityInspector<TwitterVertex, DefaultEdge>(
				twitterGraph);

		vertxForUser = findVertexWithUser(twitterGraph, "4201323615");
		System.out.println(vertxForUser.size());

		for (final TwitterVertex userVertex : vertxForUser) {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					drawTwitterGraph(new ListenableDirectedGraph<>(
							new DirectedSubgraph<>(twitterGraph2, inspector.connectedSetOf(userVertex), null)));
				}
			});
		}

		Map<String, AtomicInteger> countMap = new HashMap<String, AtomicInteger>();

		for (TwitterVertex vertex : twitterGraph2.vertexSet()) {
			if (countMap.containsKey(vertex.getUserId())) {
				countMap.get(vertex.getUserId()).incrementAndGet();
			} else {
				countMap.put(vertex.getUserId(), new AtomicInteger(1));
			}
		}

		for (String key : countMap.keySet()) {
			System.out.println("User: " + key + " count: " + countMap.get(key));
		}

		Map<String, AtomicInteger> countMap2 = new HashMap<String, AtomicInteger>();

		for (TwitterVertex vertex : twitterGraph.vertexSet()) {
			if (countMap2.containsKey(vertex.getUserId())) {
				countMap2.get(vertex.getUserId()).incrementAndGet();
			} else {
				countMap2.put(vertex.getUserId(), new AtomicInteger(1));
			}
		}

		System.out.println(countMap.keySet().size());
		System.out.println(countMap2.keySet().size());

		for (String key : countMap2.keySet()) {
			if (countMap.containsKey(key)) {
				if (countMap.get(key).get() != countMap2.get(key).get()) {
					// System.out.println(
					// "User: " + key + " before: " + countMap.get(key) + "
					// after: " + countMap2.get(key));
				}
			} else {
				// System.out.println("New User: " + key + " count: " +
				// countMap2.get(key));
			}
		}

		// findSameCommunicationInGraph(inspector.connectedSets());

		Map<String, Set<Integer>> frequentUser = findFrequentUsers(inspector.connectedSets());

		int max = 0;
		String maxUser = "";
		for (String user : frequentUser.keySet()) {
			if (frequentUser.get(user).size() > max && !user.equalsIgnoreCase("N/A")) {
				max = frequentUser.get(user).size();
				maxUser = user;
			}
		}
		System.out.println(maxUser + " count: " + max);

		// drawVertexById(twitterGraph, 5534, 68, 219, 312, 413, 492);
	}

	private static Map<String, Set<Integer>> findFrequentUsers(List<Set<TwitterVertex>> connectedSets) {
		Map<String, Set<Integer>> userInGraphMap = new HashMap<String, Set<Integer>>();
		for (Set<TwitterVertex> set : connectedSets) {

			List<String> userInGraph = getUserFromGraph(set);
			for (String user : userInGraph) {
				if (!userInGraphMap.containsKey(user)) {
					Set<Integer> posSet = new HashSet<Integer>();
					posSet.add(connectedSets.indexOf(set));
					userInGraphMap.put(user, posSet);
				} else {
					Set<Integer> posSet = userInGraphMap.get(user);
					posSet.add(connectedSets.indexOf(set));
					userInGraphMap.put(user, posSet);
				}
			}
		}
		return userInGraphMap;
	}

	private static void drawVertexById(final DefaultDirectedGraph<TwitterVertex, DefaultEdge> twitterGraph,
			int... ids) {
		final ConnectivityInspector<TwitterVertex, DefaultEdge> inspector = new ConnectivityInspector<TwitterVertex, DefaultEdge>(
				twitterGraph);
		for (final int id : ids) {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					drawTwitterGraph(new ListenableDirectedGraph<>(
							new DirectedSubgraph<>(twitterGraph, inspector.connectedSets().get(id), null)));
				}
			});
		}
	}

	private static Set<TwitterVertex> findVertexWithUser(DefaultDirectedGraph<TwitterVertex, DefaultEdge> twitterGraph,
			String id) {
		Set<TwitterVertex> temp = new HashSet<TwitterVertex>();
		for (TwitterVertex vertex : twitterGraph.vertexSet()) {
			if (vertex.getUserId().equalsIgnoreCase(id)) {
				temp.add(vertex);
			}
		}
		return temp;
	}

	private static void findSameCommunicationInGraph(List<Set<TwitterVertex>> connectedSets) {

		for (Set<TwitterVertex> set : connectedSets) {

			List<String> userInGraph = getUserFromGraph(set);

			for (Set<TwitterVertex> set2 : connectedSets) {
				if (!set.equals(set2)) {
					int counter = 0;
					for (TwitterVertex vertex : set2) {
						if (userInGraph.contains(vertex.getUserId())
								&& !vertex.getTweetMessage().equalsIgnoreCase("N/A")) {
							counter++;
						}
						if (counter > 1) {
							// System.out.println("More than one same user in: "
							// + connectedSets.indexOf(set) + " -->"
							// + connectedSets.indexOf(set2));
						}

					}
				}
			}

		}
	}

	private static void findSameUserInGraphs(List<Set<TwitterVertex>> connectedSets) {
		for (Set<TwitterVertex> set : connectedSets) {

			List<String> userInGraph = getUserFromGraph(set);
			for (Set<TwitterVertex> set2 : connectedSets) {
				if (!set2.equals(set)) {
					for (TwitterVertex vertex : set2) {
						if (userInGraph.contains(vertex.getUserId())
								&& !vertex.getTweetMessage().equalsIgnoreCase("N/A")) {
							// System.out.println("Same user in: " +
							// connectedSets.indexOf(set) + " -->"
							// + connectedSets.indexOf(set2));
						}
					}
				}
			}
		}

	}

	private static List<String> getUserFromGraph(Set<TwitterVertex> set) {
		List<String> userList = new ArrayList<String>();
		for (TwitterVertex vertex : set) {
			if (!vertex.getTweetMessage().equalsIgnoreCase("N/A"))
				userIdToName.put(vertex.getUserId(), vertex.getUserScreenName());
			userList.add(vertex.getUserId());
		}
		return userList;
	}

	private static String readFile(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));
		return new String(encoded, StandardCharsets.UTF_8);
	}

	protected static void drawTwitterGraph(ListenableDirectedGraph<TwitterVertex, DefaultEdge> graph) {
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
}
