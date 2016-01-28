package de.hs.osnabrueck.tenbeitel.mr.association.io;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ItemSet {

	private String[] values;

	public ItemSet(String[] itemSet) {
		values = itemSet;
	}

	public String[] get() {
		// TODO Auto-generated method stub
		return values;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(values);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemSet other = (ItemSet) obj;
		if (!Arrays.deepEquals(values, other.get()))
			return false;
		return true;
	}

	@Override
	public String toString() {

		return Arrays.toString(values);
	}

}
