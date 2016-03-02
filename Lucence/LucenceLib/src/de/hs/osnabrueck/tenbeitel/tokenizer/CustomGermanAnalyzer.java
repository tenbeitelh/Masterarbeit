<<<<<<< HEAD:Lucence/src/de/hs/osnabrueck/tenbeitel/tokenizer/CustomGermanAnalyzer.java
package de.hs.osnabrueck.tenbeitel.tokenizer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

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
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;

import de.hs.osnabrueck.tenbeitel.tokenizer.enumeration.StemmingType;

/**
 * Custom Implemenation of the GermanAnalyzer of Lucence v5. It is necessary to
 * get controll of the stemming and tokenization process
 * 
 * @author H. Tenbeitel
 *
 */
public final class CustomGermanAnalyzer extends StopwordAnalyzerBase {

	/** File containing default German stopwords. */
	public final static String DEFAULT_STOPWORD_FILE = "german_stop.txt";

	/**
	 * Returns a set of default German-stopwords
	 * 
	 * @return a set of default German-stopwords
	 */
	public static final CharArraySet getDefaultStopSet() {
		return DefaultSetHolder.DEFAULT_SET;
	}

	private static class DefaultSetHolder {
		private static final CharArraySet DEFAULT_SET;

		static {
			try {
				DEFAULT_SET = WordlistLoader.getSnowballWordSet(
						IOUtils.getDecodingReader(SnowballFilter.class, DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
			} catch (IOException ex) {
				// default set should always be present as it is part of the
				// distribution (JAR)
				throw new RuntimeException("Unable to load default stopword set");
			}
		}
	}

	/**
	 * Contains the stopwords used with the {@link StopFilter}.
	 */

	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	private final CharArraySet exclusionSet;

	/**
	 * Builds an analyzer with the default stop words:
	 * {@link #getDefaultStopSet()}.
	 */
	public CustomGermanAnalyzer() {
		this(DefaultSetHolder.DEFAULT_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param stopwords
	 *            a stopword set
	 */
	public CustomGermanAnalyzer(CharArraySet stopwords) {
		this(stopwords, CharArraySet.EMPTY_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param stopwords
	 *            a stopword set
	 * @param stemExclusionSet
	 *            a stemming exclusion set
	 */
	public CustomGermanAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(stopwords);
		exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
	}

	/**
	 * Creates {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
	 * used to tokenize all the text in the provided {@link Reader}.
	 * 
	 * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
	 *         built from a {@link StandardTokenizer} filtered with
	 *         {@link StandardFilter}, {@link LowerCaseFilter},
	 *         {@link StopFilter} , {@link SetKeywordMarkerFilter} if a stem
	 *         exclusion set is provided, {@link GermanNormalizationFilter} and
	 *         {@link GermanLightStemFilter}
	 */
	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		final Tokenizer source = new StandardTokenizer();;
		// ==> deprecated
		// if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
		// source = new StandardTokenizer();
		// } else {
		// source = new StandardTokenizer40();
		// }
		TokenStream result = new StandardFilter(source);

		return new TokenStreamComponents(source, result);
	}

	public TokenStream labelKeywordsInStream(TokenStream stream) {
		stream = new SetKeywordMarkerFilter(stream, exclusionSet);
		return stream;
	}

	public TokenStream tokenStreamToLowerCase(TokenStream stream) {
		stream = new LowerCaseFilter(stream);
		return stream;
	}

	public TokenStream removeStopWordsFromTokenStream(TokenStream stream) {
		stream = new StopFilter(stream, stopwords);
		return stream;
	}

	public TokenStream stemTokenStream(TokenStream stream, StemmingType type) {
		switch (type) {
		case GermanLightStemFilter:
			stream = new GermanNormalizationFilter(stream);
			break;
		case GermanMinimalStemFilter:
			stream = new GermanStemFilter(stream);
			break;
		case GermanStemFilter:
			stream = new GermanLightStemFilter(stream);
			break;
		default:
			stream = new GermanLightStemFilter(stream);
			break;
		}
		return stream;
	}


=======
package de.hs.osnabrueck.tenbeitel.tokenizer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;

import de.hs.osnabrueck.tenbeitel.tokenizer.enumeration.StemmingType;

/**
 * Custom Implemenation of the GermanAnalyzer of Lucence v5. It is necessary to
 * get controll of the stemming and tokenization process
 * 
 * @author H. Tenbeitel
 *
 */
public final class CustomGermanAnalyzer extends StopwordAnalyzerBase {

	/** File containing default German stopwords. Provided by lucence lib*/
	public final static String DEFAULT_STOPWORD_FILE = "german_stop.txt";

	/**
	 * Returns a set of default German-stopwords
	 * 
	 * @return a set of default German-stopwords
	 */
	public static final CharArraySet getDefaultStopSet() {
		return DefaultSetHolder.DEFAULT_SET;
	}

	private static class DefaultSetHolder {
		private static final CharArraySet DEFAULT_SET;

		static {
			try {
				DEFAULT_SET = WordlistLoader.getSnowballWordSet(
						IOUtils.getDecodingReader(SnowballFilter.class, DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
			} catch (IOException ex) {
				// default set should always be present as it is part of the
				// distribution (JAR)
				throw new RuntimeException("Unable to load default stopword set");
			}
		}
	}

	/**
	 * Contains the stopwords used with the {@link StopFilter}.
	 */

	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	private final CharArraySet exclusionSet;

	/**
	 * Builds an analyzer with the default stop words:
	 * {@link #getDefaultStopSet()}.
	 */
	public CustomGermanAnalyzer() {
		this(DefaultSetHolder.DEFAULT_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param stopwords
	 *            a stopword set
	 */
	public CustomGermanAnalyzer(CharArraySet stopwords) {
		this(stopwords, CharArraySet.EMPTY_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param stopwords
	 *            a stopword set
	 * @param stemExclusionSet
	 *            a stemming exclusion set
	 */
	public CustomGermanAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(stopwords);
		exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
	}

	/**
	 * Creates {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
	 * used to tokenize all the text in the provided {@link Reader}.
	 * 
	 * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
	 *         built from a {@link StandardTokenizer} filtered with
	 *         {@link StandardFilter}, {@link LowerCaseFilter},
	 *         {@link StopFilter} , {@link SetKeywordMarkerFilter} if a stem
	 *         exclusion set is provided, {@link GermanNormalizationFilter} and
	 *         {@link GermanLightStemFilter}
	 */
	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		final Tokenizer source = new StandardTokenizer();;
		// ==> deprecated
		// if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
		// source = new StandardTokenizer();
		// } else {
		// source = new StandardTokenizer40();
		// }
		TokenStream result = new StandardFilter(source);

		return new TokenStreamComponents(source, result);
	}
	
	public TokenStream limitTokenLength(TokenStream stream, int minValue, int maxValue){
		TokenStream result = new LengthFilter(stream, minValue, maxValue);
		return result;
	}

	public TokenStream labelKeywordsInStream(TokenStream stream) {
		stream = new SetKeywordMarkerFilter(stream, exclusionSet);
		return stream;
	}

	public TokenStream tokenStreamToLowerCase(TokenStream stream) {
		stream = new LowerCaseFilter(stream);
		return stream;
	}

	public TokenStream removeStopWordsFromTokenStream(TokenStream stream) {
		stream = new StopFilter(stream, stopwords);
		return stream;
	}

	public TokenStream stemTokenStream(TokenStream stream, StemmingType type) {
		switch (type) {
		case GermanLightStemFilter:
			stream = new GermanNormalizationFilter(stream);
			break;
		case GermanMinimalStemFilter:
			stream = new GermanStemFilter(stream);
			break;
		case GermanStemFilter:
			stream = new GermanLightStemFilter(stream);
			break;
		default:
			stream = new GermanLightStemFilter(stream);
			break;
		}
		return stream;
	}


>>>>>>> fdfee6bca5ef6de15c2654e6fc072da1f0681430:Lucence/LucenceLib/src/de/hs/osnabrueck/tenbeitel/tokenizer/CustomGermanAnalyzer.java
}