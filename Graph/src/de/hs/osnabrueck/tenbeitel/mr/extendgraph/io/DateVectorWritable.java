package de.hs.osnabrueck.tenbeitel.mr.extendgraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

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
	public String toString() {
		return "DateVectorWritable [date=" + date.toString() + ", namedVector=" + namedVector.toString() + "]";
	}

}
