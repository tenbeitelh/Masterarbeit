package de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.part;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.TextPair;

public class KeyPartitioner extends Partitioner<TextPair, Text> {
	@Override
	public int getPartition(/* [ */TextPair key, Text value, int numPartitions) {
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}