package de.hs.osnabrueck.tenbeitel.mr.association.mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
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

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSet;
import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;
import de.hs.osnabrueck.tenbeitel.mr.association.utils.AprioriUtils;

public class FrequentItemsSetMapper extends Mapper<Text, StringTuple, StringTuple, IntWritable> {
	private static Integer actualItemSetLength = 0;

	private static Set<List<String>> itemSets = new HashSet<List<String>>();
	private static Set<List<String>> candidates;

	private static StringTuple itemValue = new StringTuple();
	private static final IntWritable countWritable = new IntWritable(1);

	@Override
	protected void map(Text key, StringTuple value, Context context) throws IOException, InterruptedException {
		List<String> tokens = value.getEntries();
		for (List<String> candidate : candidates) {
			System.out.println(candidate.toString());
			if (tokens.containsAll(candidate)) {
				context.write(new StringTuple(candidate), countWritable);
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

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();

		actualItemSetLength = conf.getInt("apriori.itemset_lenght", actualItemSetLength);
		System.out.println(actualItemSetLength);

		readPreviousFrequentItemSets(context.getCacheFiles(), conf);

		candidates = AprioriUtils.generateCandidates(itemSets, actualItemSetLength);
		System.out.println(candidates.size());
		for (List<String> candidate : candidates) {
			System.out.println(candidate);
		}

		try {
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
		} finally {
			cleanup(context);
		}
	}

	private void readPreviousFrequentItemSets(URI[] filePaths, Configuration conf) throws IOException {
		System.out.println("Cached files: " + filePaths.length);
		for (int i = 0; i < filePaths.length; i++) {

			Option fileOption = SequenceFile.Reader.file(new Path(filePaths[i]));
			try (SequenceFile.Reader frequentItemsReader = new SequenceFile.Reader(conf, fileOption)) {

				NullWritable key = NullWritable.get();
				StringTuple value = new StringTuple();

				while (frequentItemsReader.next(key, value)) {
					itemSets.add(value.getEntries());
				}
			}
		}
		System.out.println(itemSets.size());
	}

}
