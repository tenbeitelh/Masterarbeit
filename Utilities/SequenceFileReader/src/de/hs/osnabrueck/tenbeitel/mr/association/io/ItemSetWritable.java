package de.hs.osnabrueck.tenbeitel.mr.association.io;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class ItemSetWritable extends ArrayWritable implements WritableComparable<ItemSetWritable> {

	public ItemSetWritable() {
		super(Text.class);
	}

	public ItemSetWritable(String[] itemSet) {
		super(Text.class);
		Arrays.sort(itemSet);
		Text textItems[] = new Text[itemSet.length];
		for (int i = 0; i < itemSet.length; i++) {
			textItems[i] = new Text(itemSet[i]);
		}
		set(textItems);
	}

	public String[] getStringItemSet() {
		Writable[] writableItemSet = get();
		String[] stringItemSet = new String[writableItemSet.length];
		for (int i = 0; i < writableItemSet.length; i++) {
			stringItemSet[i] = ((Text) writableItemSet[i]).toString();
		}
		return stringItemSet;
	}

	@Override
	public int hashCode() {
		Text[] objects = (Text[]) get();
		int hash = 0;
		for (Text object : objects) {
			hash += object.hashCode();
		}
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return true;
	}

	@Override
	public int compareTo(ItemSetWritable o) {
		Writable[] thisText = this.get();
		Writable[] otherText = o.get();
		int thisLength = thisText.length;
		int otherLength = otherText.length;
		int min = Math.min(thisLength, otherLength);
		for (int i = 0; i < min; i++) {
			int ret = ((Text) thisText[i]).compareTo((Text) otherText[i]);
			if (ret != 0) {
				return ret;
			}
		}
		if (thisLength < otherLength) {
			return -1;
		} else if (thisLength > otherLength) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Writable wa : get()) {
			sb.append(((Text) wa).toString());
			sb.append(" ");
		}
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

}
