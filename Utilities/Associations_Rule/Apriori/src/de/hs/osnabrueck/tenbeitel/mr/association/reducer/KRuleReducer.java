package de.hs.osnabrueck.tenbeitel.mr.association.reducer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.StringTuple;

import com.google.common.collect.Lists;

import de.hs.osnabrueck.tenbeitel.mr.association.utils.AprioriUtils;

public class KRuleReducer extends Reducer<StringTuple, IntWritable, Text, DoubleWritable> {
	private static Double minConf = 0.65;
	private static HashMap<StringTuple, IntWritable> itemSetMap = new HashMap<StringTuple, IntWritable>();

	private static Text associationRule = new Text();
	private static DoubleWritable confidenceWritable;

	@Override
	protected void reduce(StringTuple key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		List<String> itemSetZ = key.getEntries();
		Collections.sort(itemSetZ);
		Integer supportOfItemSet = values.iterator().next().get();

		Set<List<String>> thenParts = new HashSet<List<String>>();
		for (String item : itemSetZ) {
			List<String> zWithOutItem = new ArrayList<String>(itemSetZ);
			zWithOutItem.remove(item);
			Collections.sort(zWithOutItem);

			Double confidencePercent = new Double(supportOfItemSet)
					/ new Double(itemSetMap.get(new StringTuple(zWithOutItem)).get());
			System.out.println(itemSetZ.toString());
			System.out.println(supportOfItemSet);
			System.out.println(zWithOutItem.toString());
			System.out.println(itemSetMap.get(new StringTuple(zWithOutItem)).get());
			System.out.println(confidencePercent);
			if (confidencePercent >= minConf) {
				List<String> temp = new ArrayList<String>();
				temp.add(item);
				thenParts.add(temp);
				associationRule
						.set(new StringTuple(zWithOutItem).toString() + " ==> " + new StringTuple(item).toString());
				System.out.println(associationRule.toString());
				confidenceWritable = new DoubleWritable(confidencePercent);
				context.write(associationRule, confidenceWritable);
			}
		}

		for (int i = 2; i < itemSetZ.size() - 1; i++) {
			thenParts = AprioriUtils.generateCandidates(thenParts, i);
			if (thenParts.isEmpty()) {
				break;
			}
			for (List<String> tuple : thenParts) {
				List<String> zWithOutItem = new ArrayList<String>(itemSetZ);
				zWithOutItem.removeAll(tuple);

				Collections.sort(zWithOutItem);

				Double confidencePercent = new Double(supportOfItemSet)
						/ new Double(itemSetMap.get(new StringTuple(zWithOutItem)).get());

				System.out.println(itemSetZ.toString());
				System.out.println(supportOfItemSet);
				System.out.println(zWithOutItem.toString());
				System.out.println(itemSetMap.get(new StringTuple(zWithOutItem)).get());
				System.out.println(confidencePercent);

				if (confidencePercent >= minConf) {
					associationRule.set(
							new StringTuple(zWithOutItem).toString() + " ==> " + new StringTuple(tuple).toString());
					confidenceWritable = new DoubleWritable(confidencePercent);
					System.out.println(associationRule.toString());
					context.write(associationRule, confidenceWritable);
				} else {
					thenParts.remove(tuple);
				}
			}
		}

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		minConf = context.getConfiguration().getDouble("apriori.min_confidence", minConf);
		readFrequentItemSets(context.getCacheFiles(), context.getConfiguration());
		System.out.println(itemSetMap.toString());

	}

	private void readFrequentItemSets(URI[] filePaths, Configuration conf) throws IOException {

		for (int i = 0; i < filePaths.length; i++) {

			Option fileOption = SequenceFile.Reader.file(new Path(filePaths[i]));
			try (SequenceFile.Reader frequentItemsReader = new SequenceFile.Reader(conf, fileOption)) {

				StringTuple key = new StringTuple();
				IntWritable value = new IntWritable();

				while (frequentItemsReader.next(key, value)) {
					itemSetMap.put(new StringTuple(key.getEntries()), new IntWritable(value.get()));
				}
			}

		}

		System.out.println(itemSetMap.toString());
	}

}
