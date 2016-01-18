package de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.NamedVector;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.utils.DateUtils;

public class CalculateDistanceReducer extends Reducer<IntWritable, DateVectorWritable, Text, Text> {

	private static final CosineDistanceMeasure MEASURE = new CosineDistanceMeasure();

	private static final String PRE_POSTFIX = "_pre";
	private static final String POST_POSTFIX = "_post";

	private static Double similarityTreshold = 0.9;

	private Text keyId;
	private Text valueId;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		String tresholdString = context.getConfiguration().get("reducer.treshold", "0.9");
		try {
			similarityTreshold = Double.valueOf(tresholdString);
		} catch (NumberFormatException ex) {
			System.out.println("Could not parse command line paramter reducer.treshold=[" + tresholdString
					+ "] into double. Using default treshold of 0.9");
		}
	}

	@Override
	protected void reduce(IntWritable key, Iterable<DateVectorWritable> values, Context context)
			throws IOException, InterruptedException {
		keyId = new Text();
		valueId = new Text();
		for (DateVectorWritable currentVector : values) {
			DateVectorWritable cVector = new DateVectorWritable(currentVector);
			for (DateVectorWritable calcVector : values) {
				NamedVector currentNamedVector = (NamedVector) cVector.getNamedVector().getVector();
				NamedVector calcNamedVector = (NamedVector) calcVector.getNamedVector().getVector();
				if (!currentNamedVector.getName().equals(calcNamedVector.getName())) {
					Double similarity = MEASURE.distance(currentNamedVector, calcNamedVector);
					System.out.println(
							currentNamedVector.getName() + " <-> " + calcNamedVector.getName() + " = " + similarity);
					if (similarity >= similarityTreshold) {
						// TODO Date check
						try {
							Date currentVectorDate = DateUtils
									.convertTwitterDateStringToDate(cVector.getDate().toString());
							Date calcVectorDate = DateUtils
									.convertTwitterDateStringToDate(currentVector.getDate().toString());

							System.out.println(currentVectorDate + " - " + calcVectorDate);

							if (calcVectorDate.before(currentVectorDate)) {
								keyId.set(currentNamedVector.getName() + PRE_POSTFIX);
								valueId.set(calcNamedVector.getName());
								context.write(keyId, valueId);
							}
							if (calcVectorDate.after(currentVectorDate)) {
								keyId.set(currentNamedVector.getName() + POST_POSTFIX);
								valueId.set(calcNamedVector.getName());
								context.write(keyId, valueId);
							}

						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}

	}

}
