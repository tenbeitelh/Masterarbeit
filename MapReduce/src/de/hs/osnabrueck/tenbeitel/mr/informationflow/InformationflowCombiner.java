package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.google.gson.Gson;

public class InformationflowCombiner extends Reducer<Text, Text, Text, Text> {
	private static final Gson GSON = new Gson();

	// private static Set<String> processedIds;
	// private static Map<String, String[]> replyMap;
	private static DefaultDirectedGraph<String, DefaultEdge> graph;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// InformationflowCombiner.processedIds = new HashSet<String>();
		// InformationflowCombiner.replyMap = new HashMap<String, String[]>();

		InformationflowCombiner.graph = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		// for (String key : InformationflowCombiner.replyMap.keySet()) {
		// for (String linkedTo : InformationflowCombiner.replyMap.get(key)) {
		// if (!InformationflowCombiner.graph.containsVertex(linkedTo)) {
		// InformationflowCombiner.graph.addVertex(linkedTo);
		// }
		// InformationflowCombiner.graph.addEdge(linkedTo, key);
		//
		// }
		// }

		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream so = new ObjectOutputStream(bo);
			so.writeObject(InformationflowCombiner.graph);
			so.flush();
			context.write(context.getCurrentKey(), new Text(bo.toString()));
		} catch (Exception e) {
			System.out.println(e);
		}

		// String json =
		// InformationflowCombiner.GSON.toJson(InformationflowCombiner.graph);
		// System.out.println(json);
		// context.write(context.getCurrentKey(), new Text(json));

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String statusIdString = key.toString();
		InformationflowCombiner.graph.addVertex(statusIdString);

		for (Text linkedTo : values) {
			String linkedToString = linkedTo.toString();
			InformationflowCombiner.graph.addVertex(linkedToString);
			InformationflowCombiner.graph.addEdge(linkedToString, statusIdString);
		}

	}

}
