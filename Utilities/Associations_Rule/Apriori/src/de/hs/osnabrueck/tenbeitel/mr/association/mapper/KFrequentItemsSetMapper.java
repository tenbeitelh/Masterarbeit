package de.hs.osnabrueck.tenbeitel.mr.association.mapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.tenbeitel.mr.association.utils.AprioriUtils;

public class KFrequentItemsSetMapper extends Mapper<Text, StringTuple, StringTuple, IntWritable> {
	private static Integer actualItemSetLength = 0;

	private static Set<List<String>> itemSets = new HashSet<List<String>>();
	private static List<List<String>> candidates;

	private static StringTuple itemValue = new StringTuple();
	private static final IntWritable countWritable = new IntWritable(1);

	@Override
	protected void map(Text key, StringTuple value, Context context) throws IOException, InterruptedException {
		List<String> tokens = new ArrayList<String>(value.getEntries());
		Collections.sort(tokens);
		for (List<String> candidate : candidates) {
			if (tokens.containsAll(candidate)) {
				context.write(new StringTuple(candidate), countWritable);
			}
		}
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();

		actualItemSetLength = conf.getInt("apriori.itemset_lenght", actualItemSetLength);

		readPreviousFrequentItemSets(context.getCacheFiles(), conf);

		candidates = new ArrayList<List<String>>(AprioriUtils.generateCandidates(itemSets, actualItemSetLength));

		Collections.sort(candidates, new Comparator<List<String>>() {

			@Override
			public int compare(List<String> o1, List<String> o2) {
				return o1.get(0).compareTo(o2.get(0));
			}
		});

		try {
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
		} finally {
			cleanup(context);
		}
	}

	private void readPreviousFrequentItemSets(URI[] filePaths, Configuration conf) throws IOException {

		for (int i = 0; i < filePaths.length; i++) {

			Option fileOption = SequenceFile.Reader.file(new Path(filePaths[i]));
			try (SequenceFile.Reader frequentItemsReader = new SequenceFile.Reader(conf, fileOption)) {

				StringTuple key = new StringTuple();
				IntWritable value = new IntWritable();

				while (frequentItemsReader.next(key, value)) {
					itemSets.add(key.getEntries());
				}
			}
		}
	}

	// @Override
	// protected void setup(Context context) throws IOException,
	// InterruptedException {
	// super.setup(context);
	// Configuration conf = context.getConfiguration();
	//
	// actualItemSetLength = conf.getInt("apriori.itemset_lenght",
	// actualItemSetLength);
	// System.out.println(actualItemSetLength);
	//
	// readPreviousFrequentItemSets(context.getCacheFiles(), conf);
	//
	// candidates = AprioriUtils.generateCandidates(itemSets,
	// actualItemSetLength);
	// System.out.println(candidates.size());
	// for (List<String> candidate : candidates) {
	// System.out.println(candidate);
	// }
	// }

}
