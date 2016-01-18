package de.hs.osnabrueck.tenbeitel.mr.extendgraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

public class DateVectorWritable implements Writable {

	private Text date;
	private WeightedPropertyVectorWritable namedVector;

	public DateVectorWritable() {
		this.date = new Text();
		this.namedVector = new WeightedPropertyVectorWritable();
	}

	public DateVectorWritable(Text date, WeightedPropertyVectorWritable namedVector) {
		this.date = date;
		this.namedVector = namedVector;
	}

	public DateVectorWritable(DateVectorWritable delegate) {
		this.date = new Text(delegate.getDate());
		WeightedPropertyVectorWritable delegateVector = delegate.getNamedVector();
		this.namedVector = new WeightedPropertyVectorWritable(delegateVector.getWeight(), delegateVector.getVector(),
				delegateVector.getProperties());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		date.write(out);
		namedVector.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date.readFields(in);
		namedVector.readFields(in);
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	public WeightedPropertyVectorWritable getNamedVector() {
		return namedVector;
	}

	public void setNamedVector(WeightedPropertyVectorWritable namedVector) {
		this.namedVector = namedVector;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((namedVector == null) ? 0 : namedVector.hashCode());
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
		DateVectorWritable other = (DateVectorWritable) obj;
		NamedVector otherNVector = (NamedVector) other.getNamedVector().getVector();
		if (!((NamedVector) namedVector.getVector()).getName().equals(otherNVector.getName())) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "DateVectorWritable [date=" + date.toString() + ", namedVector=" + namedVector.toString() + "]";
	}

}
