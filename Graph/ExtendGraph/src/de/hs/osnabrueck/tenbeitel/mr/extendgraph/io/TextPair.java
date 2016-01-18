package de.hs.osnabrueck.tenbeitel.mr.extendgraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair implements WritableComparable<TextPair> {

	Text first;
	Text second;

	public TextPair() {
		this.first = new Text();
		this.second = new Text();
	}

	public TextPair(String first, String second) {
		this.first = new Text(first);
		this.second = new Text(second);
	}

	public TextPair(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {

			return cmp;
		}
		return second.compareTo(tp.second);

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair tp = (TextPair) obj;
			return first.equals(tp.first) && second.equals(tp.getSecond());
		}
		return false;
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	@Override
	public String toString() {
		return "TextPair [first=" + first.toString() + ", second=" + second.toString() + "]";
	}

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);

				if (cmp != 0) {
					return cmp;
				}

				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);

			} catch (IOException e) {
				throw new IllegalArgumentException();
			}
		}

		static {
			WritableComparator.define(TextPair.class, new Comparator());
		}

	}

	public static class FirstComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public FirstComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).first.compareTo(((TextPair) b).first);
			}
			return super.compare(a, b);
		}
	}

}
