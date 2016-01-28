package de.hs.osnabrueck.tenbeitel.mr.association.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;

import de.hs.osnabrueck.tenbeitel.mr.association.utils.AprioriUtils;
/**
 * 
 * @author HTenbeitel
 *
 */
public class ItemSetWritable implements WritableComparable<ItemSetWritable> {
	
	private Writable[] values;
	private Class<? extends Writable> valueClass;

	public ItemSetWritable(Class<? extends Writable> valueClass) {
		
		if (valueClass == null) {
			throw new IllegalArgumentException("null valueClass");
		}
		this.valueClass = valueClass;
	}

	public ItemSetWritable() {
		this(Text.class);
	}

	public ItemSetWritable(String[] itemSet) {
		this(Text.class);
		Arrays.sort(itemSet);
		Text textItems[] = new Text[itemSet.length];
		for (int i = 0; i < itemSet.length; i++) {
			textItems[i] = new Text(itemSet[i]);
		}

		set(textItems);
	}

	public ItemSetWritable(ItemSetWritable delegate) {
		this(Text.class);
		Writable[] delegateArray = delegate.get();
		values = new Text[delegateArray.length];
		for (int i = 0; i < delegateArray.length; i++) {
			values[i] = new Text((Text) delegateArray[i]);
		}
	}

	public void set(Writable[] values) {
		this.values = values;
	}
	
	

	public Writable[] get() {
		return values;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		values = new Writable[in.readInt()]; // construct values
		for (int i = 0; i < values.length; i++) {
			Writable value = WritableFactories.newInstance(valueClass);
			value.readFields(in); // read a value
			values[i] = value; // store it in values
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.length); // write values
		for (int i = 0; i < values.length; i++) {
			values[i].write(out);
		}
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
		Writable[] objects = get();
		int hash = 0;
		for (Writable object : objects) {
			hash += ((Text) object).hashCode();
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
