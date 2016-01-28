package de.hs.osnabrueck.tenbeitel.mr.association.io;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class ItemSetWritable extends ArrayWritable {
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
		Text[] writableItemSet = (Text[]) get();
		String[] stringItemSet = new String[writableItemSet.length];
		for (int i = 0; i < writableItemSet.length; i++) {
			stringItemSet[i] = writableItemSet[i].toString();
		}
		return stringItemSet;
	}
}
