package de.hs.osnabrueck.tenbeitel.mr.extendgraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

public class ClusterDateVectorWritable extends DateVectorWritable {
	private Text clusterId;

	public ClusterDateVectorWritable() {
		super();
		this.clusterId = new Text();
	}

	public ClusterDateVectorWritable(Text clusterId, Text date, WeightedPropertyVectorWritable namedVector) {
		super(date, namedVector);
		this.clusterId = clusterId;
	}

	public ClusterDateVectorWritable(ClusterDateVectorWritable next) {

		super(new Text(next.getDate()), new WeightedPropertyVectorWritable(next.getNamedVector().getWeight(),
				next.getNamedVector().getVector(), next.getNamedVector().getProperties()));
		this.clusterId = new Text(next.getClusterId());
	}

	public Text getClusterId() {
		return clusterId;
	}

	public void setClusterId(Text clusterId) {
		this.clusterId = clusterId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		super.write(out);
		clusterId.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		super.readFields(in);
		clusterId.readFields(in);
	}

	@Override
	public String toString() {
		return "ClusterDateVectorWritable [clusterId=" + clusterId + ", date=" + getDate().toString() + ", namedVector="
				+ getNamedVector().toString() + "]";
	}

	public DateVectorWritable toDateVectorWritable() {
		return new DateVectorWritable(this.getDate(), this.getNamedVector());
	}

}
