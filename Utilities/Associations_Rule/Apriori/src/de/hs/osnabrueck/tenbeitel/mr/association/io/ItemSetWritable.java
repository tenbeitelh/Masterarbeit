package de.hs.osnabrueck.tenbeitel.mr.association.io;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import de.hs.osnabrueck.tenbeitel.mr.association.utils.AprioriUtils;

public class ItemSetWritable extends ArrayWritable {
	private String test;

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
		ItemSetWritable other = (ItemSetWritable) obj;
		String[] thisTextObjects = getStringItemSet();
		String[] textObjects = other.getStringItemSet();
		return AprioriUtils.arrayContainsSubset(thisTextObjects, textObjects);
	}

}
