package de.hs.osnabrueck.tenbeitel.mahout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.vectorizer.document.SequenceFileTokenizerMapper;


public class CustomDocumentProcessor {
	public static final String TOKENIZED_DOCUMENT_OUTPUT_FOLDER = "tokenized-documents";
	  public static final String ANALYZER_CLASS = "analyzer.class";

	  /**
	   * Cannot be initialized. Use the static functions
	   */
	  private CustomDocumentProcessor() {

	  }
	  
	  /**
	   * Convert the input documents into token array using the {@link StringTuple} The input documents has to be
	   * in the {@link org.apache.hadoop.io.SequenceFile} format
	   * 
	   * @param input
	   *          input directory of the documents in {@link org.apache.hadoop.io.SequenceFile} format
	   * @param output
	   *          output directory were the {@link StringTuple} token array of each document has to be created
	   * @param analyzerClass
	   *          The Lucene {@link Analyzer} for tokenizing the UTF-8 text
	   */
	  public static void tokenizeDocuments(Path input,
	                                       Class<? extends Analyzer> analyzerClass,
	                                       Path output,
	                                       Configuration baseConf)
	    throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration(baseConf);
	    // this conf parameter needs to be set enable serialisation of conf values
	    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
	                                  + "org.apache.hadoop.io.serializer.WritableSerialization"); 
	    conf.set(ANALYZER_CLASS, analyzerClass.getName());
	    
	    Job job = new Job(conf);
	    job.setJobName("DocumentProcessor::DocumentTokenizer: input-folder: " + input);
	    job.setJarByClass(CustomDocumentProcessor.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(StringTuple.class);
	    FileInputFormat.setInputPaths(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.setMapperClass(SequenceFileTokenizerMapper.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    HadoopUtil.delete(conf, output);

	    boolean succeeded = job.waitForCompletion(true);
	    if (!succeeded) {
	      throw new IllegalStateException("Job failed!");
	    }

	  }
	}
