package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import com.google.common.collect.Iterators;
import com.google.gson.Gson;

public class InformationflowCombiner extends Reducer<Text, Text, Text, Text> {
	private static final Gson GSON = new Gson();

	// private static Set<String> processedIds;
	private static Map<String, String[]> replyMap;
	private static Graph<String, DefaultEdge> graph;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// InformationflowCombiner.processedIds = new HashSet<String>();
		InformationflowCombiner.replyMap = new HashMap<String, String[]>();

		InformationflowCombiner.graph = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		for (String key : InformationflowCombiner.replyMap.keySet()) {
			for (String linkedTo : InformationflowCombiner.replyMap.get(key)) {
				if (!InformationflowCombiner.graph.containsVertex(linkedTo)) {
					InformationflowCombiner.graph.addVertex(linkedTo);
				}
				InformationflowCombiner.graph.addEdge(linkedTo, key);

			}
		}
		String json = InformationflowCombiner.GSON.toJson(InformationflowCombiner.graph);
		context.write(context.getCurrentKey(), new Text(json));

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text[] tempArray = Iterators.toArray(values.iterator(), Text.class);
		String[] idArray = new String[tempArray.length];

		for (int i = 0; i < idArray.length; i++) {
			idArray[i] = tempArray[i].toString();
		}

		String keyString = key.toString();
		InformationflowCombiner.replyMap.put(keyString, idArray);
		InformationflowCombiner.graph.addVertex(keyString);

	}

}
