package de.hs.osnabrueck.tenbeitel.mahout.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.German2Stemmer;

public class GermanStemAnalyzer extends CustomAnalyzer {
	
	public GermanStemAnalyzer(Version matchVersion) {
		super(matchVersion);
	}

	
	public GermanStemAnalyzer(Version matchVersion, CharArraySet stopwords) {
		super(matchVersion, stopwords, CharArraySet.EMPTY_SET);
	}

	public GermanStemAnalyzer(Version matchVersion, CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(matchVersion, stopwords, stemExclusionSet);
		
	}

	
	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		final Tokenizer source = new StandardTokenizer(matchVersion, reader);
		TokenStream result = new StandardFilter(matchVersion, source);
		result = new LowerCaseFilter(matchVersion, result);
		result = new StopFilter(matchVersion, result, stopwords);
		result = new SetKeywordMarkerFilter(result, exclusionSet);
		if (matchVersion.onOrAfter(Version.LUCENE_36)) {
			result = new GermanNormalizationFilter(result);
			result = new GermanLightStemFilter(result);
		} else if (matchVersion.onOrAfter(Version.LUCENE_31)) {
			result = new SnowballFilter(result, new German2Stemmer());
		} else {
			result = new GermanStemFilter(result);
		}
		return new TokenStreamComponents(source, result);
	}
}
