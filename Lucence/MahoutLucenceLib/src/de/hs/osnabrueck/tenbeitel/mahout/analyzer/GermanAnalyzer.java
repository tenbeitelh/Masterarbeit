package de.hs.osnabrueck.tenbeitel.mahout.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

public class GermanAnalyzer extends CustomAnalyzer {

	public GermanAnalyzer(Version matchVersion) {
		super(matchVersion);
		// TODO Auto-generated constructor stub
	}
	
	public GermanAnalyzer(Version matchVersion, CharArraySet stopwords) {
		super(matchVersion, stopwords, CharArraySet.EMPTY_SET);
	}

	
	public GermanAnalyzer(Version matchVersion, CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(matchVersion, stopwords);
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		final Tokenizer source = new StandardTokenizer(matchVersion, reader);
		TokenStream result = new StandardFilter(matchVersion, source);
		result = new LowerCaseFilter(matchVersion, result);
		result = new StopFilter(matchVersion, result, stopwords);
		result = new LengthFilter(matchVersion, result, 3, Integer.MAX_VALUE);
		return new TokenStreamComponents(source, result);
	}
}
