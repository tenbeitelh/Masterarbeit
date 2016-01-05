package de.hs.osnabrueck.tenbeitel.mahout;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.lucene.AnalyzerUtils;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.document.SequenceFileTokenizerMapper;

import com.google.common.io.Closeables;

public class CustomSequenceFileTokenizerMapper extends SequenceFileTokenizerMapper {
	private Analyzer analyzer;

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, StringTuple>.Context context)
			throws IOException, InterruptedException {
		System.out.println("Key: " + key.toString() + " - Value: " + value.toString());
		TokenStream stream = analyzer.tokenStream(key.toString(), new StringReader(value.toString()));
		CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
		stream.reset();
		StringTuple document = new StringTuple();
		while (stream.incrementToken()) {
			if (termAtt.length() > 0) {
				document.add(new String(termAtt.buffer(), 0, termAtt.length()));
			}
		}
		stream.end();
		Closeables.close(stream, true);
		context.write(key, document);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		String analyzerClassName = context.getConfiguration().get(DocumentProcessor.ANALYZER_CLASS,
				StandardAnalyzer.class.getName());
		try {
			analyzer = AnalyzerUtils.createAnalyzer(analyzerClassName);
		} catch (ClassNotFoundException e) {
			throw new IOException("Unable to create analyzer: " + analyzerClassName, e);
		}
	}

}
